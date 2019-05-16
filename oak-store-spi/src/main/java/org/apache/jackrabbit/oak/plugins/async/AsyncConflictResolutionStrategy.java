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
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public interface AsyncConflictResolutionStrategy {

    void resolveAddExistingProperty(NodeBuilder builder, PropertyState before, PropertyState after);

    void resolveChangeDeletedProperty(NodeBuilder builder, PropertyState after, PropertyState base);

    void resolveChangeChangedProperty(NodeBuilder builder, PropertyState before, PropertyState after);

    void resolveDeleteDeletedProperty(NodeBuilder builder, PropertyState before);

    void resolveDeleteChangedProperty(NodeBuilder builder, PropertyState before);

    void resolveAddExistingNode(NodeBuilder builder, String name, NodeState before, NodeState after);

    void resolveChangeDeletedNode(NodeBuilder builder, String name, NodeState after, NodeState base);

    void resolveDeleteDeletedNode(NodeBuilder builder, String name, NodeState before);

    void resolveDeleteChangedNode(NodeBuilder builder, String name, NodeState before);
}
