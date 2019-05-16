/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.async;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The MemoryAsyncNodeStore introduces a concept of asynchronous commits.
 * <p/>
 * An asynchronous commit is a change set that contains only nodes which are,
 * themselves or any of its parent, marked with a mixin type oak:Async.
 * <p/>
 * The current simplistic solution knows only 1 global strategy that it applies
 * to all async changes. More sophisticated solutions should support configuring
 * the actual resolution type on a per node, nodeType, user, etc basis.
 * <p/>
 * To support asynchronous commits, this implementation keeps async changes in a
 * MemoryNodeStore and flushes to the underlying NodeStore when it needs to. The
 * intention is that it does flush every few seconds at latest. If the flushes
 * cause a conflict, the implementation (not done yet) will resolve them and do
 * another merge. If this doesn't succeed after a few iterations the current
 * idea is that it would fallback to a more pessimistic approach where it would
 * set a cluster wide lock (which every instance polls once a second), then stop
 * flushing until that lock is released (all of this is not implemented yet).
 */
public class MemoryAsyncNodeStore implements NodeStore {

    // TODO: this should become a mixin
    public static final String ASYNC_PROPERTY_NAME = "oak:async";

    private final NodeStore underlying;

    private final AsyncConflictResolutionStrategy asyncConflictResolutionStrategy;

    private NodeState asyncBase;

    private MemoryNodeStore asyncStore;

    private CommitHook syncCheckHook = new SyncCheckHook();

    public MemoryAsyncNodeStore(NodeStore underlying, AsyncConflictResolutionStrategy asyncConflictResolutionStrategy) {
        this.underlying = checkNotNull(underlying);
        this.asyncConflictResolutionStrategy = checkNotNull(asyncConflictResolutionStrategy);
        initialFetch();
    }

    private void initialFetch() {
        this.asyncBase = underlying.getRoot();
        this.asyncStore = new MemoryNodeStore(asyncBase);
    }

    @Override
    public @NotNull NodeState getRoot() {
        return asyncStore.getRoot();
    }

    /**
     * Merges the builder to the detached branch, ie not the underlying, hence
     * potentially async. CommitHooks are only executed against the detached branch.
     */
    @Override
    public @NotNull NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook,
            @NotNull CommitInfo info) throws CommitFailedException {
        try {
            return asyncStore.merge(builder, new CompositeHook(syncCheckHook, commitHook), info);
        } catch (CommitFailedException cfe) {
            if (!cfe.isOfType(CommitFailedException.MERGE) || cfe.getCode() != 100) {
                throw cfe;
            }
            // otherwise this is a sync commit and we first need to sync
            sync();

            // and then do a normal merge, without the sync-check hook
            return asyncStore.merge(builder, commitHook, info);
        }
    }

    @Override
    public @NotNull NodeState rebase(@NotNull NodeBuilder builder) {
        return asyncStore.rebase(builder);
    }

    @Override
    public NodeState reset(@NotNull NodeBuilder builder) {
        return asyncStore.reset(builder);
    }

    @Override
    public @NotNull Blob createBlob(InputStream inputStream) throws IOException {
        // blobs aren't async, so use the underlying directly
        return underlying.createBlob(inputStream);
    }

    @Override
    public @Nullable Blob getBlob(@NotNull String reference) {
        // blobs aren't async, so use the underlying directly
        return underlying.getBlob(reference);
    }

    /**
     * Pushes the local changes to the underlying node store while setting the local
     * branched store to the underlying root
     */
    void sync() {
        try {
            // do a diff between
            // - asyncStore.getRoot()
            // and
            // - asyncBase
            // and apply the diff to a new branch based on the current underlying.getRoot
            NodeState to = asyncStore.getRoot();
            NodeState from = asyncBase;
            NodeBuilder underlyingDiffBuilder = underlying.getRoot().builder();
            AsyncConflictResolutionRebaseDiff diff = new AsyncConflictResolutionRebaseDiff(underlyingDiffBuilder,
                    asyncConflictResolutionStrategy);

            // TODO: add a retry
            if (!to.compareAgainstBaseState(from, diff)) {
                throw new Error("compareAgainstBaseState failed");
            }
            underlying.merge(underlyingDiffBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // the local changes are now in the underlying
            // what's left is resetting the async store to match the underyling
            // which can be achieved via (1) undo the local changes, (2) fetch the
            // underlying changes

            // so: (1) undo
            to = asyncBase;
            from = asyncStore.getRoot();
            NodeBuilder undoDiffBuilder = asyncStore.getRoot().builder();
            if (!to.compareAgainstBaseState(from, new ConflictAnnotatingRebaseDiff(undoDiffBuilder))) {
                throw new Error("compareAgainstBaseState failed");
            }
            // TODO: undo shouldn't cause any conflicts - but still check in merge
            asyncStore.merge(undoDiffBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // (2) apply the underlying changes
            to = underlying.getRoot();
            from = asyncBase;
            NodeBuilder fetchRebaseDiffBuilder = asyncStore.getRoot().builder();
            diff = new AsyncConflictResolutionRebaseDiff(fetchRebaseDiffBuilder, asyncConflictResolutionStrategy);
            if (!to.compareAgainstBaseState(from, diff)) {
                throw new Error("compareAgainstBaseState failed");
            }
            asyncStore.merge(fetchRebaseDiffBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            asyncBase = to;
        } catch (CommitFailedException e) {
            throw new Error(
                    "Commit failed even though we're promising it shouldn't thanks to async conflict resolution");
        }
    }

    @Override
    public @NotNull String checkpoint(long lifetime, @NotNull Map<String, String> properties) {
        // checkpoints aren't handled async, therefore first push then use the
        // underlying
        sync();
        return underlying.checkpoint(lifetime, properties);
    }

    @Override
    public @NotNull String checkpoint(long lifetime) {
        // checkpoints aren't handled async, therefore first push then use the
        // underlying
        sync();
        return underlying.checkpoint(lifetime);
    }

    @Override
    public @NotNull Map<String, String> checkpointInfo(@NotNull String checkpoint) {
        // checkpoints aren't handled async, therefore use the underlying, no push or
        // fetch needed
        return underlying.checkpointInfo(checkpoint);
    }

    @Override
    public @NotNull Iterable<String> checkpoints() {
        // checkpoints aren't handled async, therefore use the underlying, no push or
        // fetch needed
        return underlying.checkpoints();
    }

    @Override
    public @Nullable NodeState retrieve(@NotNull String checkpoint) {
        // checkpoints aren't handled async, therefore use the underlying
        // but as this returns a nodestate, first sync
        sync();
        return underlying.retrieve(checkpoint);
    }

    @Override
    public boolean release(@NotNull String checkpoint) {
        // checkpoints aren't handled async, therefore use the underlying, no push or
        // fetch needed
        return underlying.release(checkpoint);
    }

}
