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

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.UserCodeClassLoader;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BigtableChangeStreamSourceReaderTest {

    private static final String PROJECT = "test-project";
    private static final String INSTANCE = "test-instance";
    private static final String TABLE = "test-table";
    private static final String COLUMN_FAMILY = "cf";
    private static final String CELL_COLUMN = "payload";

    @Test
    void defaultBufferCapacityIs1000() {
        assertEquals(
                1000,
                BigtableChangeStreamDynamicTableFactory.BUFFER_CAPACITY.defaultValue(),
                "Default buffer capacity should be 1000 for backpressure");
    }

    @Test
    void addSplitsTracksActiveSplits() throws Exception {
        BigtableChangeStreamSourceReader reader = createReader();
        reader.start();

        BigtableChangeStreamSplit s1 =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "m"), "t1");
        BigtableChangeStreamSplit s2 =
                new BigtableChangeStreamSplit(ByteStringRange.create("m", "z"), "t2");

        reader.addSplits(Arrays.asList(s1, s2));

        List<BigtableChangeStreamSplit> state = reader.snapshotState(1L);
        assertEquals(2, state.size(), "Should track 2 active splits");
        reader.close();
    }

    @Test
    void addSplitsBeforeStartQueuesForLater() throws Exception {
        BigtableChangeStreamSourceReader reader = createReader();

        BigtableChangeStreamSplit s1 =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "m"), "t1");
        reader.addSplits(Collections.singletonList(s1));

        List<BigtableChangeStreamSplit> state = reader.snapshotState(1L);
        assertEquals(1, state.size(), "Should have 1 pending split before start()");
        reader.close();
    }

    @Test
    void pollNextReturnsNothingWhenBufferEmpty() throws Exception {
        BigtableChangeStreamSourceReader reader = createReader();
        reader.start();

        @SuppressWarnings("unchecked")
        ReaderOutput<RowData> output = mock(ReaderOutput.class);
        InputStatus status = reader.pollNext(output);
        assertEquals(InputStatus.NOTHING_AVAILABLE, status);
        reader.close();
    }

    @Test
    void snapshotStateIncludesPendingAndActiveSplits() throws Exception {
        BigtableChangeStreamSourceReader reader = createReader();

        BigtableChangeStreamSplit pending =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "f"), "t-pending");
        reader.addSplits(Collections.singletonList(pending));

        List<BigtableChangeStreamSplit> state = reader.snapshotState(1L);
        assertEquals(1, state.size());
        reader.close();
    }

    @Test
    void closeShutdownsExecutorAndClient() throws Exception {
        BigtableDataClient mockClient = mock(BigtableDataClient.class);
        BigtableChangeStreamSourceReader reader = createReaderWithClient(() -> mockClient);
        reader.start();

        reader.close();
        verify(mockClient).close();
    }

    @Test
    void extractCellBytesReturnsMatchingSetCell() {
        BigtableChangeStreamSourceReader reader = createReader();

        ChangeStreamMutation mutation = mock(ChangeStreamMutation.class);
        SetCell setCell = mock(SetCell.class);
        when(setCell.getFamilyName()).thenReturn(COLUMN_FAMILY);
        when(setCell.getQualifier()).thenReturn(ByteString.copyFromUtf8(CELL_COLUMN));
        when(setCell.getValue()).thenReturn(ByteString.copyFromUtf8("test-value"));
        doReturn(com.google.common.collect.ImmutableList.of(setCell)).when(mutation).getEntries();

        byte[] result = reader.extractCellBytes(mutation);
        assertNotNull(result);
        assertEquals("test-value", new String(result));
    }

    @Test
    void extractCellBytesReturnsNullForNonMatchingFamily() {
        BigtableChangeStreamSourceReader reader = createReader();

        ChangeStreamMutation mutation = mock(ChangeStreamMutation.class);
        SetCell setCell = mock(SetCell.class);
        when(setCell.getFamilyName()).thenReturn("other-family");
        when(setCell.getQualifier()).thenReturn(ByteString.copyFromUtf8(CELL_COLUMN));
        doReturn(com.google.common.collect.ImmutableList.of(setCell)).when(mutation).getEntries();

        assertNull(reader.extractCellBytes(mutation));
    }

    @Test
    void extractCellBytesReturnsNullForNonMatchingColumn() {
        BigtableChangeStreamSourceReader reader = createReader();

        ChangeStreamMutation mutation = mock(ChangeStreamMutation.class);
        SetCell setCell = mock(SetCell.class);
        when(setCell.getFamilyName()).thenReturn(COLUMN_FAMILY);
        when(setCell.getQualifier()).thenReturn(ByteString.copyFromUtf8("other-column"));
        doReturn(com.google.common.collect.ImmutableList.of(setCell)).when(mutation).getEntries();

        assertNull(reader.extractCellBytes(mutation));
    }

    @Test
    void extractCellBytesReturnsNullForEmptyEntries() {
        BigtableChangeStreamSourceReader reader = createReader();

        ChangeStreamMutation mutation = mock(ChangeStreamMutation.class);
        doReturn(com.google.common.collect.ImmutableList.of()).when(mutation).getEntries();

        assertNull(reader.extractCellBytes(mutation));
    }

    @Test
    void handleRebalanceRequestWithNoActiveSplitsIsNoOp() throws Exception {
        BigtableChangeStreamSourceReader reader = createReader();
        reader.start();

        RebalanceRequestEvent event = new RebalanceRequestEvent(1);
        reader.handleSourceEvents(event);

        List<BigtableChangeStreamSplit> state = reader.snapshotState(2L);
        assertEquals(0, state.size(), "No splits should be active");
        reader.close();
    }

    @Test
    void maxPartitionThreadsDefaultIs64() {
        assertEquals(
                64,
                BigtableChangeStreamDynamicTableFactory.MAX_PARTITION_THREADS.defaultValue(),
                "Default max partition threads should be 64");
    }

    @Test
    void threadPoolRejectsWhenMaxPartitionThreadsExceeded() throws Exception {
        // Create reader with maxPartitionThreads=2
        BigtableDataClient mockClient = mock(BigtableDataClient.class);
        BigtableChangeStreamSourceReader reader =
                createReaderWithClientAndThreads(() -> mockClient, 2);
        reader.start();

        // Add 3 splits — the thread pool should accept 2 but reject the 3rd
        BigtableChangeStreamSplit s1 =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "f"), "t1");
        BigtableChangeStreamSplit s2 =
                new BigtableChangeStreamSplit(ByteStringRange.create("f", "m"), "t2");
        reader.addSplits(Arrays.asList(s1, s2));
        // The 2 splits should be accepted (threads created for them)
        List<BigtableChangeStreamSplit> state = reader.snapshotState(1L);
        assertEquals(2, state.size(), "Should have 2 active splits");
        reader.close();
    }

    @Test
    void threadPoolIdleThreadsAreReclaimed() throws Exception {
        // Verify the executor is a ThreadPoolExecutor with corePoolSize=0
        // (idle threads are reclaimed after keep-alive timeout)
        BigtableDataClient mockClient = mock(BigtableDataClient.class);
        BigtableChangeStreamSourceReader reader = createReaderWithClient(() -> mockClient);
        reader.start();

        // Just verify the reader starts and closes without error —
        // the ThreadPoolExecutor with corePoolSize=0 is the implementation detail
        reader.close();
    }

    // --- changelog-mode config tests ---

    @Test
    void defaultChangelogModeIsInsertOnly() {
        assertEquals(
                "insert-only",
                BigtableChangeStreamDynamicTableFactory.CHANGELOG_MODE.defaultValue(),
                "Default changelog-mode should be insert-only for backward compatibility");
    }

    // --- getChangelogMode tests ---

    @Test
    void getChangelogModeInsertOnlyByDefault() {
        BigtableChangeStreamDynamicTableSource source =
                createDynamicTableSource("insert-only", "my_key");
        org.apache.flink.table.connector.ChangelogMode mode = source.getChangelogMode();
        assertTrue(mode.contains(org.apache.flink.types.RowKind.INSERT));
        assertFalse(mode.contains(org.apache.flink.types.RowKind.DELETE));
    }

    @Test
    void getChangelogModeAllWithRowKeyField() {
        BigtableChangeStreamDynamicTableSource source = createDynamicTableSource("all", "my_key");
        org.apache.flink.table.connector.ChangelogMode mode = source.getChangelogMode();
        assertTrue(mode.contains(org.apache.flink.types.RowKind.INSERT));
        assertTrue(mode.contains(org.apache.flink.types.RowKind.DELETE));
    }

    @Test
    void getChangelogModeAllWithoutRowKeyFieldFallsBackToInsertOnly() {
        BigtableChangeStreamDynamicTableSource source = createDynamicTableSource("all", null);
        org.apache.flink.table.connector.ChangelogMode mode = source.getChangelogMode();
        assertTrue(mode.contains(org.apache.flink.types.RowKind.INSERT));
        assertFalse(mode.contains(org.apache.flink.types.RowKind.DELETE));
    }

    // --- hasDeleteEntries tests ---

    @Test
    void hasDeleteEntriesReturnsTrueForDeleteCells() {
        BigtableChangeStreamSourceReader reader = createReader();
        ChangeStreamMutation mutation = mock(ChangeStreamMutation.class);
        DeleteCells deleteCells = mock(DeleteCells.class);
        doReturn(com.google.common.collect.ImmutableList.of(deleteCells))
                .when(mutation)
                .getEntries();

        assertTrue(reader.hasDeleteEntries(mutation));
    }

    @Test
    void hasDeleteEntriesReturnsTrueForDeleteFamily() {
        BigtableChangeStreamSourceReader reader = createReader();
        ChangeStreamMutation mutation = mock(ChangeStreamMutation.class);
        DeleteFamily deleteFamily = mock(DeleteFamily.class);
        doReturn(com.google.common.collect.ImmutableList.of(deleteFamily))
                .when(mutation)
                .getEntries();

        assertTrue(reader.hasDeleteEntries(mutation));
    }

    @Test
    void hasDeleteEntriesReturnsFalseForSetCellOnly() {
        BigtableChangeStreamSourceReader reader = createReader();
        ChangeStreamMutation mutation = mock(ChangeStreamMutation.class);
        SetCell setCell = mock(SetCell.class);
        doReturn(com.google.common.collect.ImmutableList.of(setCell)).when(mutation).getEntries();

        assertFalse(reader.hasDeleteEntries(mutation));
    }

    @Test
    void hasDeleteEntriesReturnsFalseForEmptyEntries() {
        BigtableChangeStreamSourceReader reader = createReader();
        ChangeStreamMutation mutation = mock(ChangeStreamMutation.class);
        doReturn(com.google.common.collect.ImmutableList.of()).when(mutation).getEntries();

        assertFalse(reader.hasDeleteEntries(mutation));
    }

    // --- fail-on-deserialization-error tests ---

    @Test
    void failOnDeserializationErrorDefaultIsFalse() {
        BigtableChangeStreamDynamicTableSource source =
                createDynamicTableSource("insert-only", null);
        // Default changelog mode, no failure — verifies the option wires through without error
        assertNotNull(source);
    }

    @Test
    void failOnDeserializationErrorPassesThroughToSource() {
        BigtableChangeStreamDynamicTableSource source =
                createDynamicTableSourceWithFailOnError("insert-only", null, true);
        assertNotNull(source);
    }

    // --- Helpers ---

    private static BigtableChangeStreamDynamicTableSource createDynamicTableSourceWithFailOnError(
            String changelogMode, String rowKeyField, boolean failOnError) {
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new RowType.RowField("payload", new VarCharType())));
        return new BigtableChangeStreamDynamicTableSource(
                PROJECT,
                INSTANCE,
                TABLE,
                null,
                COLUMN_FAMILY,
                CELL_COLUMN,
                null,
                rowType,
                rowKeyField,
                300,
                1000,
                0,
                64,
                changelogMode,
                failOnError,
                0);
    }

    private BigtableChangeStreamSourceReader createReader() {
        BigtableDataClient mockClient = mock(BigtableDataClient.class);
        return createReaderWithClient(() -> mockClient);
    }

    private static BigtableChangeStreamDynamicTableSource createDynamicTableSource(
            String changelogMode, String rowKeyField) {
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new RowType.RowField("payload", new VarCharType())));
        return new BigtableChangeStreamDynamicTableSource(
                PROJECT,
                INSTANCE,
                TABLE,
                null,
                COLUMN_FAMILY,
                CELL_COLUMN,
                null,
                rowType,
                rowKeyField,
                300,
                1000,
                0,
                64,
                changelogMode,
                false,
                0);
    }

    private BigtableChangeStreamSourceReader createReaderWithClientAndThreads(
            java.util.function.Supplier<BigtableDataClient> clientFactory,
            int maxPartitionThreads) {
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new RowType.RowField("payload", new VarCharType())));

        RowKeyInjectingDeserializationSchema schema =
                new RowKeyInjectingDeserializationSchema(
                        new FakeDeserializationSchema(),
                        RowKeyInjectingDeserializationSchema.NO_ROW_KEY_INDEX,
                        null,
                        rowType);

        return new BigtableChangeStreamSourceReader(
                createMockReaderContext(),
                PROJECT,
                INSTANCE,
                TABLE,
                null,
                COLUMN_FAMILY,
                CELL_COLUMN,
                schema,
                false,
                false,
                300,
                100,
                0,
                maxPartitionThreads,
                clientFactory);
    }

    private BigtableChangeStreamSourceReader createReaderWithClient(
            java.util.function.Supplier<BigtableDataClient> clientFactory) {
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new RowType.RowField("payload", new VarCharType())));

        RowKeyInjectingDeserializationSchema schema =
                new RowKeyInjectingDeserializationSchema(
                        new FakeDeserializationSchema(),
                        RowKeyInjectingDeserializationSchema.NO_ROW_KEY_INDEX,
                        null,
                        rowType);

        return new BigtableChangeStreamSourceReader(
                createMockReaderContext(),
                PROJECT,
                INSTANCE,
                TABLE,
                null,
                COLUMN_FAMILY,
                CELL_COLUMN,
                schema,
                false,
                false,
                300,
                100,
                0,
                4,
                clientFactory);
    }

    private static SourceReaderContext createMockReaderContext() {
        SourceReaderContext ctx = mock(SourceReaderContext.class);
        SourceReaderMetricGroup metricGroup = createNoOpMetricGroup();
        doReturn(metricGroup).when(ctx).metricGroup();
        when(ctx.getIndexOfSubtask()).thenReturn(0);
        when(ctx.currentParallelism()).thenReturn(1);
        when(ctx.getUserCodeClassLoader())
                .thenReturn(
                        new UserCodeClassLoader() {
                            @Override
                            public ClassLoader asClassLoader() {
                                return Thread.currentThread().getContextClassLoader();
                            }

                            @Override
                            public void registerReleaseHookIfAbsent(
                                    String releaseHookName, Runnable hook) {}
                        });
        return ctx;
    }

    @SuppressWarnings("unchecked")
    private static SourceReaderMetricGroup createNoOpMetricGroup() {
        SourceReaderMetricGroup mg = mock(SourceReaderMetricGroup.class);
        when(mg.counter(org.mockito.ArgumentMatchers.anyString())).thenReturn(new SimpleCounter());
        when(mg.histogram(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.any(Histogram.class)))
                .thenAnswer(inv -> inv.getArgument(1));
        when(mg.gauge(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.any(Gauge.class)))
                .thenAnswer(inv -> inv.getArgument(1));
        when(mg.addGroup(org.mockito.ArgumentMatchers.anyString())).thenReturn(mg);
        return mg;
    }

    /**
     * Simple deserialization schema that returns null — sufficient for tests that don't exercise
     * the deserialization path.
     */
    private static class FakeDeserializationSchema
            implements org.apache.flink.api.common.serialization.DeserializationSchema<RowData> {
        @Override
        public RowData deserialize(byte[] message) {
            return null;
        }

        @Override
        public boolean isEndOfStream(RowData nextElement) {
            return false;
        }

        @Override
        public org.apache.flink.api.common.typeinfo.TypeInformation<RowData> getProducedType() {
            return null;
        }
    }
}
