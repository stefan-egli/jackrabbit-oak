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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public final class AsyncConflictResolutionRebaseDiff extends ConflictAnnotatingRebaseDiff {

    /** if true uses the AsyncConflictResolutionStrategy on this node */
    private final boolean useAsyncConflictResolution;

    /** the strategy for async conflict resolution - must always succeed */
    private final AsyncConflictResolutionStrategy strategy;

    public AsyncConflictResolutionRebaseDiff(NodeBuilder builder, AsyncConflictResolutionStrategy strategy) {
        super(builder);
        this.useAsyncConflictResolution = builder.hasProperty(MemoryAsyncNodeStore.ASYNC_PROPERTY_NAME);
        this.strategy = strategy;
    }

    private AsyncConflictResolutionRebaseDiff(NodeBuilder builder, AsyncConflictResolutionStrategy strategy,
            boolean asyncResolveConflicts) {
        super(builder);
        this.useAsyncConflictResolution = asyncResolveConflicts;
        this.strategy = strategy;
    }

    @Override
    protected ConflictAnnotatingRebaseDiff createDiff(NodeBuilder builder, String name) {
        final NodeBuilder childBuilder = builder.child(name);
        final boolean childAsyncResolveConflicts;
        if (useAsyncConflictResolution) {
            childAsyncResolveConflicts = true;
        } else if (!childBuilder.hasProperty(MemoryAsyncNodeStore.ASYNC_PROPERTY_NAME)) {
            childAsyncResolveConflicts = false;
        } else {
            // then from here on down we no longer trigger any conflicts but
            // resolve them
            childAsyncResolveConflicts = true;
        }
        return new AsyncConflictResolutionRebaseDiff(childBuilder, strategy, childAsyncResolveConflicts);
    }

    @Override
    protected void addExistingProperty(NodeBuilder builder, PropertyState before, PropertyState after) {
        if (!useAsyncConflictResolution) {
            super.addExistingProperty(builder, before, after);
        } else {
            strategy.resolveAddExistingProperty(builder, before, after);
        }
    }

    @Override
    protected void changeDeletedProperty(NodeBuilder builder, PropertyState after, PropertyState base) {
        if (!useAsyncConflictResolution) {
            super.changeDeletedProperty(builder, after, base);
        } else {
            strategy.resolveChangeDeletedProperty(builder, after, base);
        }
    }

    @Override
    protected void changeChangedProperty(NodeBuilder builder, PropertyState before, PropertyState after) {
        if (!useAsyncConflictResolution) {
            super.changeChangedProperty(builder, before, after);
        } else {
            strategy.resolveChangeChangedProperty(builder, before, after);
        }
    }

    @Override
    protected void deleteDeletedProperty(NodeBuilder builder, PropertyState before) {
        if (!useAsyncConflictResolution) {
            super.deleteDeletedProperty(builder, before);
        } else {
            strategy.resolveDeleteDeletedProperty(builder, before);
        }
    }

    @Override
    protected void deleteChangedProperty(NodeBuilder builder, PropertyState before) {
        if (!useAsyncConflictResolution) {
            super.deleteChangedProperty(builder, before);
        } else {
            strategy.resolveDeleteChangedProperty(builder, before);
        }
    }

    @Override
    protected void addExistingNode(NodeBuilder builder, String name, NodeState before, NodeState after) {
        if (!useAsyncConflictResolution) {
            super.addExistingNode(builder, name, before, after);
        } else {
            strategy.resolveAddExistingNode(builder, name, before, after);
        }
    }

    @Override
    protected void changeDeletedNode(NodeBuilder builder, String name, NodeState after, NodeState base) {
        if (!useAsyncConflictResolution) {
            super.changeDeletedNode(builder, name, after, base);
        } else {
            strategy.resolveChangeDeletedNode(builder, name, after, base);
        }
    }

    @Override
    protected void deleteDeletedNode(NodeBuilder builder, String name, NodeState before) {
        if (!useAsyncConflictResolution) {
            super.deleteDeletedNode(builder, name, before);
        } else {
            strategy.resolveDeleteDeletedNode(builder, name, before);
        }
    }

    @Override
    protected void deleteChangedNode(NodeBuilder builder, String name, NodeState before) {
        if (!useAsyncConflictResolution) {
            super.deleteChangedNode(builder, name, before);
        } else {
            strategy.resolveDeleteChangedNode(builder, name, before);
        }
    }

}
