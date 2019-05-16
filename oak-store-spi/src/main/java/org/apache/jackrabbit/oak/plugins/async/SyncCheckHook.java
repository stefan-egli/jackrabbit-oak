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

import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;

public class SyncCheckHook implements CommitHook {

    class SyncCheckDiff implements NodeStateDiff {

        private final boolean isAsync;

        SyncCheckDiff(NodeState parent) {
            this.isAsync = isAsync(parent);
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            return isAsync;
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            return isAsync;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            return isAsync;
        }

        private boolean isAsync(NodeState ns) {
            PropertyState asyncProperty = ns.getProperty(MemoryAsyncNodeStore.ASYNC_PROPERTY_NAME);
            if (asyncProperty == null) {
                return false;
            } else {
                return asyncProperty.getValue(BOOLEAN);
            }
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            return isAsync;
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            SyncCheckDiff diff = new SyncCheckDiff(before);
            return after.compareAgainstBaseState(before, diff);
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            return isAsync;
        }

    }

    @Override
    public @NotNull NodeState processCommit(NodeState before, NodeState after, CommitInfo info)
            throws CommitFailedException {
        SyncCheckDiff diff = new SyncCheckDiff(before);
        if (after.compareAgainstBaseState(before, diff)) {
            return after;
        } else {
            throw new CommitFailedException(CommitFailedException.MERGE, 100,
                    "Contains node without " + MemoryAsyncNodeStore.ASYNC_PROPERTY_NAME);
        }
    }

}