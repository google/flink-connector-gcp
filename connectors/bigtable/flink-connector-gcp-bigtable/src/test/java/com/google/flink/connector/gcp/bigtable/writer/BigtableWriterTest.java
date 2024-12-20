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

package com.google.flink.connector.gcp.bigtable.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import com.google.flink.connector.gcp.bigtable.serializers.GenericRecordToRowMutationSerializer;
import com.google.flink.connector.gcp.bigtable.testingutils.TestingUtils;
import com.google.flink.connector.gcp.bigtable.utils.ErrorMessages;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for the {@link BigtableSinkWriter} class.
 *
 * <p>This class verifies the functionality of the {@link BigtableSinkWriter} by testing its ability
 * to write data to a Bigtable table using a {@link GenericRecordToRowMutationSerializer}.
 *
 * <p>It uses the Bigtable emulator for testing. It also verifies the functionality of metrics
 * reporting in {@link BigtableFlushableWriter} by ensuring that metrics are accurately recorded and
 * that errors during write operations are handled correctly.
 */
public class BigtableWriterTest {
    private static final int MAX_RANGE = 10;
    private BigtableDataClient client;
    private BigtableTableAdminClient tableAdminClient;
    private BigtableFlushableWriter flushableWriter;

    WriterInitContext mockContext = mock(WriterInitContext.class);
    SinkWriter.Context mockSinkWriterContext = mock(SinkWriter.Context.class);

    @Rule public final BigtableEmulatorRule bigtableEmulator = BigtableEmulatorRule.create();

    @Before
    public void setUp() throws IOException, InterruptedException {
        BigtableTableAdminSettings.Builder tableAdminSettings =
                BigtableTableAdminSettings.newBuilderForEmulator(bigtableEmulator.getPort());
        tableAdminSettings.setProjectId(TestingUtils.PROJECT).setInstanceId(TestingUtils.INSTANCE);
        tableAdminClient = BigtableTableAdminClient.create(tableAdminSettings.build());
        tableAdminClient.createTable(
                CreateTableRequest.of(TestingUtils.TABLE).addFamily(TestingUtils.COLUMN_FAMILY));

        BigtableDataSettings.Builder dataSettings =
                BigtableDataSettings.newBuilderForEmulator(bigtableEmulator.getPort());
        dataSettings.setProjectId(TestingUtils.PROJECT).setInstanceId(TestingUtils.INSTANCE);
        this.client = BigtableDataClient.create(dataSettings.build());

        Mockito.when(mockContext.metricGroup())
                .thenReturn(UnregisteredMetricsGroup.createSinkWriterMetricGroup());

        flushableWriter = new BigtableFlushableWriter(client, mockContext, TestingUtils.TABLE);
    }

    @After
    public void close() {
        client.close();
        tableAdminClient.close();
    }

    @Test
    public void testSuccessfullWrites()
            throws IOException, InterruptedException, ExecutionException {
        Schema schema =
                SchemaBuilder.builder()
                        .record("WriterTest")
                        .fields()
                        .requiredString(TestingUtils.ROW_KEY_FIELD)
                        .requiredString(TestingUtils.STRING_FIELD)
                        .requiredInt(TestingUtils.INTEGER_FIELD)
                        .endRecord();

        GenericRecordToRowMutationSerializer serializer =
                GenericRecordToRowMutationSerializer.builder()
                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD)
                        .withColumnFamily(TestingUtils.COLUMN_FAMILY)
                        .build();

        BigtableSinkWriter<GenericRecord> writer =
                new BigtableSinkWriter<GenericRecord>(flushableWriter, serializer, mockContext);

        for (int i = 0; i < MAX_RANGE; i++) {
            GenericRecord testRecord = new GenericData.Record(schema);
            testRecord.put(TestingUtils.ROW_KEY_FIELD, "key" + i);
            testRecord.put(TestingUtils.STRING_FIELD, "string" + i);
            testRecord.put(TestingUtils.INTEGER_FIELD, i);
            writer.write(testRecord, null);
        }

        // Check no data was written
        for (int i = 0; i < MAX_RANGE; i++) {
            Row row = client.readRow(TableId.of(TestingUtils.TABLE), "key" + i);
            assertNull(row);
        }

        // Write and validate rows
        writer.flush(false);
        for (int i = 0; i < MAX_RANGE; i++) {
            Row row = client.readRow(TableId.of(TestingUtils.TABLE), "key" + i);
            assertEquals(
                    "string" + i,
                    row.getCells(TestingUtils.COLUMN_FAMILY, TestingUtils.STRING_FIELD)
                            .get(0)
                            .getValue()
                            .toStringUtf8());
            ByteString readInt =
                    row.getCells(TestingUtils.COLUMN_FAMILY, TestingUtils.INTEGER_FIELD)
                            .get(0)
                            .getValue();
            assertEquals(i, bytesToInteger(readInt));
        }

        writer.close();
    }

    @Test
    public void testExactlyOnceWrite()
            throws IOException, InterruptedException, ExecutionException {
        Schema schema =
                SchemaBuilder.builder()
                        .record("WriterTest")
                        .fields()
                        .requiredString(TestingUtils.ROW_KEY_FIELD)
                        .requiredString(TestingUtils.STRING_FIELD)
                        .requiredInt(TestingUtils.INTEGER_FIELD)
                        .endRecord();

        GenericRecordToRowMutationSerializer serializer =
                GenericRecordToRowMutationSerializer.builder()
                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD)
                        .withColumnFamily(TestingUtils.COLUMN_FAMILY)
                        .build();

        BigtableSinkWriter<GenericRecord> writer =
                new BigtableSinkWriter<GenericRecord>(flushableWriter, serializer, mockContext);

        Long fixedTime = 1734722223L;
        Mockito.when(mockSinkWriterContext.timestamp()).thenReturn(fixedTime);

        GenericRecord testRecord = new GenericData.Record(schema);
        testRecord.put(TestingUtils.ROW_KEY_FIELD, "key");
        testRecord.put(TestingUtils.STRING_FIELD, "string");
        testRecord.put(TestingUtils.INTEGER_FIELD, 1729);

        // Write same row multiple times with same timestamp
        for (int i = 0; i < MAX_RANGE; i++) {
            writer.write(testRecord, mockSinkWriterContext);
        }
        writer.flush(false);

        // Verify only one row was written
        Row row = client.readRow(TableId.of(TestingUtils.TABLE), "key");

        // Each row has a cell per qualifier, we expect two (STRING_FIELD, INTEGER_FIELD)
        assertEquals(row.getCells().size(), 2);

        assertEquals(
                "string",
                row.getCells(TestingUtils.COLUMN_FAMILY, TestingUtils.STRING_FIELD)
                        .get(0)
                        .getValue()
                        .toStringUtf8());
        ByteString readInt =
                row.getCells(TestingUtils.COLUMN_FAMILY, TestingUtils.INTEGER_FIELD)
                        .get(0)
                        .getValue();
        assertEquals(1729, bytesToInteger(readInt));
        assertEquals((long) fixedTime * 1000, row.getCells().get(0).getTimestamp());

        // Write again with new timestamp
        Long newTime = fixedTime + 10;
        Mockito.when(mockSinkWriterContext.timestamp()).thenReturn(newTime);
        GenericRecord newRecord = new GenericData.Record(schema);
        // Use same key
        newRecord.put(TestingUtils.ROW_KEY_FIELD, "key");
        newRecord.put(TestingUtils.STRING_FIELD, "newstring");
        newRecord.put(TestingUtils.INTEGER_FIELD, 92711729);

        writer.write(newRecord, mockSinkWriterContext);
        writer.flush(false);

        row = client.readRow(TableId.of(TestingUtils.TABLE), "key");
        assertEquals(row.getCells().size(), 4);
        assertEquals(
                "newstring",
                row.getCells(TestingUtils.COLUMN_FAMILY, TestingUtils.STRING_FIELD)
                        .get(0)
                        .getValue()
                        .toStringUtf8());
        readInt =
                row.getCells(TestingUtils.COLUMN_FAMILY, TestingUtils.INTEGER_FIELD)
                        .get(0)
                        .getValue();
        assertEquals(92711729, bytesToInteger(readInt));
        assertEquals((long) newTime * 1000, row.getCells().get(0).getTimestamp());

        writer.close();
    }

    @Test
    public void testSerializationErrorMetrics()
            throws IOException, InterruptedException, ExecutionException {
        Schema schema = SchemaBuilder.builder().record("WrongSchema").fields().endRecord();

        GenericRecordToRowMutationSerializer serializer =
                GenericRecordToRowMutationSerializer.builder()
                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD)
                        .withColumnFamily(TestingUtils.COLUMN_FAMILY)
                        .build();

        BigtableSinkWriter<GenericRecord> writer =
                new BigtableSinkWriter<GenericRecord>(flushableWriter, serializer, mockContext);

        GenericRecord wrongRecord = new GenericData.Record(schema);

        assertEquals(0, writer.getNumSerializationErrorsCounter().getCount());
        Assertions.assertThatThrownBy(() -> writer.write(wrongRecord, null))
                .hasMessageContaining(ErrorMessages.SERIALIZER_ERROR);

        assertEquals(1, writer.getNumSerializationErrorsCounter().getCount());
        writer.close();
    }

    @Test
    public void testCorrectMetricsFlusher()
            throws IOException, InterruptedException, ExecutionException {
        int totalBytes = 0;
        for (int i = 0; i < MAX_RANGE; i++) {
            RowMutationEntry entry = RowMutationEntry.create("key " + i);
            entry.setCell(
                    TestingUtils.COLUMN_FAMILY, "column", TestingUtils.TIMESTAMP, "field " + i);
            flushableWriter.collect(entry);
            totalBytes += BigtableFlushableWriter.getEntryBytesSize(entry);
        }

        assertEquals(0, flushableWriter.getNumRecordsOutCounter().getCount());
        assertEquals(0, flushableWriter.getNumOutEntryFailuresCounter().getCount());
        assertEquals(0, flushableWriter.getNumBatchFailuresCounter().getCount());
        assertEquals(0, flushableWriter.getNumEntriesPerFlush().getStatistics().getMax());

        flushableWriter.flush();

        assertEquals(totalBytes, flushableWriter.getNumBytesOutCounter().getCount());
        assertEquals(0, flushableWriter.getNumOutEntryFailuresCounter().getCount());
        assertEquals(0, flushableWriter.getNumBatchFailuresCounter().getCount());
        assertEquals(MAX_RANGE, flushableWriter.getNumRecordsOutCounter().getCount());
        assertEquals(MAX_RANGE, flushableWriter.getNumEntriesPerFlush().getStatistics().getMax());
        assertEquals(MAX_RANGE, flushableWriter.getNumEntriesPerFlush().getStatistics().getMin());
        assertEquals(
                (double) MAX_RANGE,
                flushableWriter.getNumEntriesPerFlush().getStatistics().getMean(),
                0.1);

        // Write again to validate Histogram
        RowMutationEntry entry = RowMutationEntry.create("key extra");
        entry.setCell(TestingUtils.COLUMN_FAMILY, "column", TestingUtils.TIMESTAMP, "field extra");
        flushableWriter.collect(entry);
        flushableWriter.flush();
        assertEquals(MAX_RANGE, flushableWriter.getNumEntriesPerFlush().getStatistics().getMax());
        assertEquals(1, flushableWriter.getNumEntriesPerFlush().getStatistics().getMin());
        assertEquals(
                (double) (MAX_RANGE + 1) / 2,
                flushableWriter.getNumEntriesPerFlush().getStatistics().getMean(),
                0.1);
    }

    @Test
    public void testErrorCounterMetricsFlusher()
            throws IOException, InterruptedException, ExecutionException {
        RowMutationEntry entry = RowMutationEntry.create("key");
        entry.setCell("Non-existen-cf", "column", TestingUtils.TIMESTAMP, "field");
        flushableWriter.collect(entry);

        RowMutationEntry entry2 = RowMutationEntry.create("key 2");
        entry2.setCell("Non-existen-cf", "column", TestingUtils.TIMESTAMP, "field2");
        flushableWriter.collect(entry2);

        assertEquals(0, flushableWriter.getNumBytesOutCounter().getCount());
        assertEquals(0, flushableWriter.getNumOutEntryFailuresCounter().getCount());
        assertEquals(0, flushableWriter.getNumBatchFailuresCounter().getCount());
        assertEquals(0, flushableWriter.getNumRecordsOutCounter().getCount());
        assertEquals(0, flushableWriter.getNumEntriesPerFlush().getStatistics().getMax());

        Assertions.assertThatThrownBy(() -> flushableWriter.flush())
                .hasMessageContaining("The 1 partial failures contained 2 entries that failed")
                .hasMessageContaining("unknown family");

        assertEquals(0, flushableWriter.getNumBytesOutCounter().getCount());
        assertEquals(2, flushableWriter.getNumOutEntryFailuresCounter().getCount());
        assertEquals(1, flushableWriter.getNumBatchFailuresCounter().getCount());
        assertEquals(0, flushableWriter.getNumRecordsOutCounter().getCount());
        assertEquals(0, flushableWriter.getNumEntriesPerFlush().getStatistics().getMax());
    }

    private int bytesToInteger(ByteString byteString) {
        return ByteBuffer.wrap(byteString.toByteArray()).getInt();
    }
}
