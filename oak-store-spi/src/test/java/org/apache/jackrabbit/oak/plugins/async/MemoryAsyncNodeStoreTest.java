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

import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

public class MemoryAsyncNodeStoreTest {

    private final Random random = new Random(1234567);

    private MemoryNodeStore underlying;
    private MemoryAsyncNodeStore async1;
    private MemoryAsyncNodeStore async2;

    @Before
    public void setup() {
        underlying = new MemoryNodeStore();
        async1 = new MemoryAsyncNodeStore(underlying, new LatestChangeWinsStrategy());
        async2 = new MemoryAsyncNodeStore(underlying, new LatestChangeWinsStrategy());
    }

    private void markAsync() throws CommitFailedException {
        NodeBuilder builder = async1.getRoot().builder();
        builder.setProperty(MemoryAsyncNodeStore.ASYNC_PROPERTY_NAME, true);
        async1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        async1.sync();
        async2.sync();
    }

    @Test
    public void simpleTest() throws CommitFailedException {
        markAsync();

        NodeBuilder builder = async1.getRoot().builder();
        assertNotNull(builder);

        NodeBuilder b = builder.child("a");
        b.child("b");
        NodeState newState = async1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertNotNull(newState);
        assertTrue(newState.hasChildNode("a"));
        assertTrue(newState.getChildNode("a").hasChildNode("b"));
    }

    @Test
    public void fetchAfterMergeTest() throws CommitFailedException {
        markAsync();

        NodeBuilder b1a = async1.getRoot().builder();
        b1a.child("a").child("b");

        assertFalse(async1.getRoot().hasChildNode("a"));
        async2.sync();
        assertFalse(async2.getRoot().hasChildNode("a"));

        async1.merge(b1a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertTrue(async1.getRoot().hasChildNode("a"));
        async2.sync();
        assertTrue(async1.getRoot().hasChildNode("a"));
        assertFalse(async2.getRoot().hasChildNode("a"));

        async1.sync();
        assertTrue(async1.getRoot().hasChildNode("a"));
        assertFalse(async2.getRoot().hasChildNode("a"));
        async2.sync();
        assertTrue(async2.getRoot().hasChildNode("a"));

        async1.sync();
        assertTrue(async1.getRoot().hasChildNode("a"));
        async2.sync();
        assertTrue(async2.getRoot().hasChildNode("a"));
    }

    @Test
    public void simpleConflictTest() throws CommitFailedException {
        markAsync();

        {
            NodeBuilder builder = async1.getRoot().builder();
            builder.child("a").setProperty("b", 1);
            async1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            assertTrue(async1.getRoot().hasChildNode("a"));
            assertEquals((Long) 1L, async1.getRoot().getChildNode("a").getProperty("b").getValue(LONG));
        }

        {
            NodeBuilder builder = async2.getRoot().builder();
            builder.child("a").setProperty("b", 2);
            async2.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            assertTrue(async2.getRoot().hasChildNode("a"));
            assertEquals((Long) 2L, async2.getRoot().getChildNode("a").getProperty("b").getValue(LONG));
        }

        async1.sync();
        assertEquals((Long) 1L, async1.getRoot().getChildNode("a").getProperty("b").getValue(LONG));
        assertEquals((Long) 2L, async2.getRoot().getChildNode("a").getProperty("b").getValue(LONG));

        async2.sync();
        assertEquals((Long) 1L, async1.getRoot().getChildNode("a").getProperty("b").getValue(LONG));
        assertEquals((Long) 2L, async2.getRoot().getChildNode("a").getProperty("b").getValue(LONG));

        async1.sync();
        assertEquals((Long) 2L, async1.getRoot().getChildNode("a").getProperty("b").getValue(LONG));
        assertEquals((Long) 2L, async2.getRoot().getChildNode("a").getProperty("b").getValue(LONG));
    }

    @Test
    public void simpleAsyncTest() throws CommitFailedException {
        markAsync();

        // prepare creating /a/b on async1
        NodeBuilder builder = async1.getRoot().builder();
        builder.child("a").child("b");

        // another session/thread on async1 shouldn't see it yet, not yet merged
        assertFalse(async1.getRoot().hasChildNode("a"));

        // now merge it on async1
        async1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // another session/thread on async1 should now see it
        assertTrue(async1.getRoot().hasChildNode("a"));

        // but async2 shouldn't see it yet
        assertFalse(async2.getRoot().hasChildNode("a"));

        // so should a 3rd one, let's create a 3rd one
        MemoryAsyncNodeStore async3 = new MemoryAsyncNodeStore(underlying, new LatestChangeWinsStrategy());
        assertFalse(async3.getRoot().hasChildNode("a"));

        // let's do another change on async1
        builder = async1.getRoot().builder();
        builder.child("d");
        async1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // async3 should see neither /a nor /d - even if it syncs
        async3.sync();
        assertFalse(async3.getRoot().hasChildNode("a"));
        assertFalse(async3.getRoot().hasChildNode("d"));

        // once we push async1's changes to the underlying, others can start seeing it
        async1.sync();

        // but async2 hasn't fetched it yet, so it still doesn't see it
        assertFalse(async2.getRoot().hasChildNode("a"));

        async2.sync();

        // after fetching, async2 should now see it
        assertTrue(async2.getRoot().hasChildNode("a"));
    }

    class Runner implements Runnable {

        final MemoryAsyncNodeStore store;
        final String id;
        final AtomicBoolean result = new AtomicBoolean(false);

        Runner(MemoryAsyncNodeStore store, int id) {
            this.store = store;
            this.id = String.valueOf(id);
        }

        @Override
        public void run() {
            try {
                NodeState root = store.getRoot();
                NodeBuilder b = root.builder();
                b.child(id);
                store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

                for (int i = 0; i < 10; i++) {
                    createRandomChildren(100);
                    store.sync();
                }
                result.set(true);
            } catch (CommitFailedException e) {
                e.printStackTrace();
                result.set(false);
            }
        }

        private void createRandomChildren(int num) throws CommitFailedException {
            for (int i = 0; i < num; i++) {
                NodeState root = store.getRoot();
                NodeBuilder b = root.builder();
                NodeBuilder base = b.child(id);
                String randomName = String.valueOf(random.nextInt(1000));
                if (!base.hasChildNode(randomName)) {
                    base.child(randomName);
                    store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                }
            }
        }

    }

    @Test
    public void asyncFlipFlopTest() throws InterruptedException, CommitFailedException {
        markAsync();

        Runner r1 = new Runner(async1, 1);
        Runner r2 = new Runner(async2, 2);
        Thread th1 = new Thread(r1);
        th1.setDaemon(true);
        Thread th2 = new Thread(r2);
        th2.setDaemon(true);
        th1.start();
        th2.start();
        th1.join(60000);
        assertFalse(th1.isAlive());
        th2.join(60000);
        assertFalse(th2.isAlive());
        assertTrue(r1.result.get());
        assertTrue(r2.result.get());
        async1.sync();
        async2.sync();
        async1.sync();
        long c11 = async1.getRoot().getChildNode("1").getChildNodeCount(-1);
        long c12 = async1.getRoot().getChildNode("2").getChildNodeCount(-1);
        long c21 = async2.getRoot().getChildNode("1").getChildNodeCount(-1);
        long c22 = async2.getRoot().getChildNode("2").getChildNodeCount(-1);
        assertTrue(c11 > 200); // usual values would be above 500, so seems safe to check for >200
        assertTrue(c22 > 200);
        assertEquals(c11, c21);
        assertEquals(c12, c22);
    }

}
