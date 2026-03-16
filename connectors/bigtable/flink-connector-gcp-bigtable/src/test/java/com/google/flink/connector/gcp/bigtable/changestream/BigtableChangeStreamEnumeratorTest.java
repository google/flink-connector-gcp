/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.flink.connector.gcp.bigtable.changestream;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BigtableChangeStreamEnumeratorTest {

    /**
     * Verifies that when the enumerator receives a PartitionChangedEvent (from a partition
     * split/merge), it enqueues the new splits and assigns them to available readers.
     */
    @Test
    void handleSourceEventAssignsNewSplitsFromPartitionChange() {
        FakeSplitEnumeratorContext ctx = new FakeSplitEnumeratorContext();
        ctx.registerReader(0, "host-0");

        // Start the enumerator with one pre-existing split (skip Bigtable API call)
        BigtableChangeStreamSplit initialSplit =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "z"), "initial-token");
        BigtableChangeStreamEnumerator enumerator =
                new BigtableChangeStreamEnumerator(
                        ctx, "proj", "inst", "tbl", Collections.singletonList(initialSplit));

        // Simulate start — assigns the initial split to reader 0
        // (We can't call start() because it creates a BigtableDataClient, but we can
        // verify handleSourceEvent independently.)

        // Manually trigger the assignment that start() would do
        enumerator.addReader(0);

        // The initial split should be assigned
        assertEquals(1, ctx.assignments.size());
        assertEquals(1, ctx.assignments.get(0).size());
        assertEquals("initial-token", ctx.assignments.get(0).get(0).getContinuationToken());

        ctx.assignments.clear();

        // Now simulate a partition split: [a,z) -> [a,m) + [m,z)
        BigtableChangeStreamSplit left =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "m"), "token-left");
        BigtableChangeStreamSplit right =
                new BigtableChangeStreamSplit(ByteStringRange.create("m", "z"), "token-right");
        PartitionChangedEvent event = new PartitionChangedEvent(Arrays.asList(left, right));

        enumerator.handleSourceEvent(0, event);

        // Both splits assigned to reader 0 (least-loaded assigns all to one reader when only one
        // exists)
        assertEquals(1, ctx.assignments.size());
        assertEquals(2, ctx.assignments.get(0).size());

        Set<String> tokens = new HashSet<>();
        tokens.add(ctx.assignments.get(0).get(0).getContinuationToken());
        tokens.add(ctx.assignments.get(0).get(1).getContinuationToken());
        assertTrue(tokens.contains("token-left"), "Expected token-left in assignments");
        assertTrue(tokens.contains("token-right"), "Expected token-right in assignments");
    }

    @Test
    void addSplitsBackRequeuesAndAssigns() {
        FakeSplitEnumeratorContext ctx = new FakeSplitEnumeratorContext();
        ctx.registerReader(0, "host-0");

        BigtableChangeStreamEnumerator enumerator =
                new BigtableChangeStreamEnumerator(
                        ctx, "proj", "inst", "tbl", Collections.emptyList());

        BigtableChangeStreamSplit split =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "z"), "tok");

        enumerator.addSplitsBack(Collections.singletonList(split), 0);

        assertEquals(1, ctx.assignments.size());
        assertEquals("tok", ctx.assignments.get(0).get(0).getContinuationToken());
    }

    @Test
    void snapshotStateCapturesPendingSplits() {
        FakeSplitEnumeratorContext ctx = new FakeSplitEnumeratorContext();
        // No readers registered — splits stay pending

        BigtableChangeStreamSplit s1 =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "m"), "t1");
        BigtableChangeStreamSplit s2 =
                new BigtableChangeStreamSplit(ByteStringRange.create("m", "z"), "t2");

        BigtableChangeStreamEnumerator enumerator =
                new BigtableChangeStreamEnumerator(
                        ctx, "proj", "inst", "tbl", Arrays.asList(s1, s2));

        Collection<BigtableChangeStreamSplit> snapshot = enumerator.snapshotState(1L);
        assertEquals(2, snapshot.size());
    }

    /** Verifies least-loaded assignment distributes splits evenly across readers. */
    @Test
    void assignsToLeastLoadedReader() {
        FakeSplitEnumeratorContext ctx = new FakeSplitEnumeratorContext();
        ctx.registerReader(0, "host-0");
        ctx.registerReader(1, "host-1");

        BigtableChangeStreamSplit s1 =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "f"), "t1");
        BigtableChangeStreamSplit s2 =
                new BigtableChangeStreamSplit(ByteStringRange.create("f", "m"), "t2");
        BigtableChangeStreamSplit s3 =
                new BigtableChangeStreamSplit(ByteStringRange.create("m", "s"), "t3");
        BigtableChangeStreamSplit s4 =
                new BigtableChangeStreamSplit(ByteStringRange.create("s", "z"), "t4");

        BigtableChangeStreamEnumerator enumerator =
                new BigtableChangeStreamEnumerator(
                        ctx, "proj", "inst", "tbl", Arrays.asList(s1, s2, s3, s4));

        enumerator.addReader(0);
        enumerator.addReader(1);

        // Each reader should get 2 splits
        List<BigtableChangeStreamSplit> reader0 =
                ctx.assignments.getOrDefault(0, Collections.emptyList());
        List<BigtableChangeStreamSplit> reader1 =
                ctx.assignments.getOrDefault(1, Collections.emptyList());
        assertEquals(2, reader0.size(), "Reader 0 should have 2 splits");
        assertEquals(2, reader1.size(), "Reader 1 should have 2 splits");
    }

    /** Verifies that adding a new reader triggers a RebalanceRequestEvent to overloaded readers. */
    @Test
    void rebalanceOnNewReader() {
        FakeSplitEnumeratorContext ctx = new FakeSplitEnumeratorContext();
        ctx.registerReader(0, "host-0");

        // Give reader 0 four splits
        BigtableChangeStreamSplit s1 =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "f"), "t1");
        BigtableChangeStreamSplit s2 =
                new BigtableChangeStreamSplit(ByteStringRange.create("f", "m"), "t2");
        BigtableChangeStreamSplit s3 =
                new BigtableChangeStreamSplit(ByteStringRange.create("m", "s"), "t3");
        BigtableChangeStreamSplit s4 =
                new BigtableChangeStreamSplit(ByteStringRange.create("s", "z"), "t4");

        BigtableChangeStreamEnumerator enumerator =
                new BigtableChangeStreamEnumerator(
                        ctx, "proj", "inst", "tbl", Arrays.asList(s1, s2, s3, s4));

        enumerator.addReader(0);
        // Reader 0 now has 4 splits
        assertEquals(4, ctx.assignments.getOrDefault(0, Collections.emptyList()).size());

        // Add a second reader — should trigger rebalance
        ctx.registerReader(1, "host-1");
        ctx.sentEvents.clear();
        enumerator.addReader(1);

        // Enumerator should have sent a RebalanceRequestEvent to reader 0
        boolean foundRebalance = false;
        for (FakeSplitEnumeratorContext.SentEvent se : ctx.sentEvents) {
            if (se.event instanceof RebalanceRequestEvent) {
                assertEquals(0, se.subtaskId, "Rebalance should target reader 0");
                foundRebalance = true;
            }
        }
        assertTrue(foundRebalance, "Expected a RebalanceRequestEvent to be sent");
    }

    /** Verifies that addSplitsBack clears the failed reader's split count. */
    @Test
    void addSplitsBackClearsReaderCount() {
        FakeSplitEnumeratorContext ctx = new FakeSplitEnumeratorContext();
        ctx.registerReader(0, "host-0");
        ctx.registerReader(1, "host-1");

        BigtableChangeStreamSplit s1 =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "m"), "t1");
        BigtableChangeStreamSplit s2 =
                new BigtableChangeStreamSplit(ByteStringRange.create("m", "z"), "t2");

        BigtableChangeStreamEnumerator enumerator =
                new BigtableChangeStreamEnumerator(
                        ctx, "proj", "inst", "tbl", Arrays.asList(s1, s2));

        enumerator.addReader(0);
        enumerator.addReader(1);
        ctx.assignments.clear();

        // Simulate reader 0 failure — its splits go back
        List<BigtableChangeStreamSplit> reader0Splits =
                Arrays.asList(
                        new BigtableChangeStreamSplit(
                                ByteStringRange.create("a", "m"), "t1-updated"));
        enumerator.addSplitsBack(reader0Splits, 0);

        // The returned split should be reassigned to reader 0 (least loaded after count reset
        // to 0, while reader 1 still has count 1)
        List<BigtableChangeStreamSplit> reader0New =
                ctx.assignments.getOrDefault(0, Collections.emptyList());
        assertEquals(1, reader0New.size(), "Returned split should be reassigned to reader 0");
        assertEquals("t1-updated", reader0New.get(0).getContinuationToken());
    }

    /** Verifies that a PartitionChangedEvent decrements the sending reader's split count. */
    @Test
    void partitionChangedDecrementsCount() {
        FakeSplitEnumeratorContext ctx = new FakeSplitEnumeratorContext();
        ctx.registerReader(0, "host-0");
        ctx.registerReader(1, "host-1");

        // Give reader 0 two splits, reader 1 one split
        BigtableChangeStreamSplit s1 =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "m"), "t1");
        BigtableChangeStreamSplit s2 =
                new BigtableChangeStreamSplit(ByteStringRange.create("m", "s"), "t2");
        BigtableChangeStreamSplit s3 =
                new BigtableChangeStreamSplit(ByteStringRange.create("s", "z"), "t3");

        BigtableChangeStreamEnumerator enumerator =
                new BigtableChangeStreamEnumerator(
                        ctx, "proj", "inst", "tbl", Arrays.asList(s1, s2, s3));

        enumerator.addReader(0);
        enumerator.addReader(1);
        ctx.assignments.clear();

        // Reader 0 reports a partition change: [a,m) -> [a,f) + [f,m)
        BigtableChangeStreamSplit left =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "f"), "t-left");
        BigtableChangeStreamSplit right =
                new BigtableChangeStreamSplit(ByteStringRange.create("f", "m"), "t-right");
        PartitionChangedEvent event = new PartitionChangedEvent(Arrays.asList(left, right));

        enumerator.handleSourceEvent(0, event);

        // The two new splits should be assigned to the least-loaded reader(s)
        int totalAssigned = 0;
        for (List<BigtableChangeStreamSplit> splits : ctx.assignments.values()) {
            totalAssigned += splits.size();
        }
        assertEquals(2, totalAssigned, "Both new splits should be assigned");
    }

    /** Verifies that SplitsReleasedEvent causes the enumerator to reassign released splits. */
    @Test
    void splitsReleasedEventReassignsSplits() {
        FakeSplitEnumeratorContext ctx = new FakeSplitEnumeratorContext();
        ctx.registerReader(0, "host-0");
        ctx.registerReader(1, "host-1");

        BigtableChangeStreamSplit s1 =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "m"), "t1");
        BigtableChangeStreamSplit s2 =
                new BigtableChangeStreamSplit(ByteStringRange.create("m", "z"), "t2");

        BigtableChangeStreamEnumerator enumerator =
                new BigtableChangeStreamEnumerator(
                        ctx, "proj", "inst", "tbl", Arrays.asList(s1, s2));

        enumerator.addReader(0);
        enumerator.addReader(1);
        ctx.assignments.clear();

        // Reader 0 releases one split
        BigtableChangeStreamSplit released =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "m"), "t1-released");
        SplitsReleasedEvent event = new SplitsReleasedEvent(Collections.singletonList(released));
        enumerator.handleSourceEvent(0, event);

        // Released split should be reassigned (to reader 0, which is now least loaded
        // after decrementing its count by 1)
        List<BigtableChangeStreamSplit> reader0New =
                ctx.assignments.getOrDefault(0, Collections.emptyList());
        assertEquals(1, reader0New.size(), "Released split should be reassigned to reader 0");
        assertEquals("t1-released", reader0New.get(0).getContinuationToken());
    }

    /**
     * Minimal fake context that captures split assignments without requiring a full Flink runtime.
     */
    private static class FakeSplitEnumeratorContext
            implements SplitEnumeratorContext<BigtableChangeStreamSplit> {

        final Map<Integer, ReaderInfo> readers = new HashMap<>();
        final Map<Integer, List<BigtableChangeStreamSplit>> assignments = new HashMap<>();
        final List<SentEvent> sentEvents = new ArrayList<>();

        /** Captures events sent to source readers. */
        static class SentEvent {
            final int subtaskId;
            final SourceEvent event;

            SentEvent(int subtaskId, SourceEvent event) {
                this.subtaskId = subtaskId;
                this.event = event;
            }
        }

        void registerReader(int subtaskId, String hostname) {
            readers.put(subtaskId, new ReaderInfo(subtaskId, hostname));
        }

        @Override
        public SplitEnumeratorMetricGroup metricGroup() {
            return new NoOpSplitEnumeratorMetricGroup();
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
            sentEvents.add(new SentEvent(subtaskId, event));
        }

        @Override
        public int currentParallelism() {
            return readers.size();
        }

        @Override
        public Map<Integer, ReaderInfo> registeredReaders() {
            return readers;
        }

        @Override
        public void assignSplit(BigtableChangeStreamSplit split, int subtaskId) {
            assignments.computeIfAbsent(subtaskId, k -> new ArrayList<>()).add(split);
        }

        @Override
        public void assignSplits(SplitsAssignment<BigtableChangeStreamSplit> newSplitAssignments) {
            newSplitAssignments
                    .assignment()
                    .forEach(
                            (subtaskId, splits) ->
                                    assignments
                                            .computeIfAbsent(subtaskId, k -> new ArrayList<>())
                                            .addAll(splits));
        }

        @Override
        public void signalNoMoreSplits(int subtask) {}

        @Override
        public <T> void callAsync(
                java.util.concurrent.Callable<T> callable,
                java.util.function.BiConsumer<T, Throwable> handler) {}

        @Override
        public <T> void callAsync(
                java.util.concurrent.Callable<T> callable,
                java.util.function.BiConsumer<T, Throwable> handler,
                long initialDelay,
                long period) {}

        @Override
        public void runInCoordinatorThread(Runnable runnable) {
            runnable.run();
        }
    }

    /** No-op metric group that silently accepts all metric registrations. */
    private static class NoOpSplitEnumeratorMetricGroup implements SplitEnumeratorMetricGroup {
        @Override
        public Counter counter(String name) {
            return new SimpleCounter();
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            return counter;
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            return gauge;
        }

        @Override
        public <H extends Histogram> H histogram(String name, H histogram) {
            return histogram;
        }

        @Override
        public <M extends Meter> M meter(String name, M meter) {
            return meter;
        }

        @Override
        public MetricGroup addGroup(String name) {
            return this;
        }

        @Override
        public MetricGroup addGroup(String key, String value) {
            return this;
        }

        @Override
        public String[] getScopeComponents() {
            return new String[0];
        }

        @Override
        public java.util.Map<String, String> getAllVariables() {
            return java.util.Collections.emptyMap();
        }

        @Override
        public String getMetricIdentifier(String metricName) {
            return metricName;
        }

        @Override
        public String getMetricIdentifier(
                String metricName, org.apache.flink.metrics.CharacterFilter filter) {
            return metricName;
        }

        @Override
        public <G extends Gauge<Long>> G setUnassignedSplitsGauge(G gauge) {
            return gauge;
        }
    }
}
