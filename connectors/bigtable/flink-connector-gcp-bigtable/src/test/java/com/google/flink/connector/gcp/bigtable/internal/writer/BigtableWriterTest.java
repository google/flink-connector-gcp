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

package com.google.flink.connector.gcp.bigtable.internal.writer;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import com.google.flink.connector.gcp.bigtable.internal.serializers.GenericRecordToRowMutationSerializer;
import com.google.flink.connector.gcp.bigtable.testingutils.TestingUtils;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for the {@link BigtableSinkWriter} class.
 *
 * <p>This class verifies the functionality of the {@link BigtableSinkWriter} by testing its ability
 * to write data to a Bigtable table using a {@link GenericRecordToRowMutationSerializer}. It uses
 * the Bigtable emulator for testing.
 */
public class BigtableWriterTest {
    private static final Long MAX_RANGE = 10L;
    private BigtableDataClient client;
    private BigtableTableAdminClient tableAdminClient;

    @Rule public final BigtableEmulatorRule bigtableEmulator = BigtableEmulatorRule.create();

    /**
     * Sets up the test environment by creating a Bigtable table in the emulator.
     *
     * @throws IOException If an error occurs while creating the table.
     */
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
    }

    /** Cleans up the test environment by closing the Bigtable clients. */
    @After
    public void close() {
        client.close();
        tableAdminClient.close();
    }

    /**
     * Tests the writing of data to a Bigtable table using the {@link BigtableSinkWriter}.
     *
     * @throws IOException If an error occurs while writing data.
     * @throws InterruptedException If the thread is interrupted while waiting for write completion.
     * @throws ExecutionException If an error occurs during the write operation.
     */
    @Test
    public void testWriter() throws IOException, InterruptedException, ExecutionException {
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
                new BigtableSinkWriter<GenericRecord>(
                        new BigtableFlushableWriter(client, null, TestingUtils.TABLE), serializer);

        for (int i = 0; i < MAX_RANGE; i++) {
            GenericRecord testRecord = new GenericData.Record(schema);
            testRecord.put(TestingUtils.ROW_KEY_FIELD, "key" + i);
            testRecord.put(TestingUtils.STRING_FIELD, "string" + i);
            testRecord.put(TestingUtils.INTEGER_FIELD, i);
            writer.write(testRecord, null);
        }
        checkEmptyData();
        writer.flush(false);
        checkWrittenData();
        writer.close();
    }

    /**
     * Helper method to verify that the Bigtable table is empty before writing data.
     *
     * @throws InterruptedException If the thread is interrupted while waiting for the read
     *     operation.
     * @throws ExecutionException If an error occurs during the read operation.
     */
    private void checkEmptyData() {
        for (int i = 0; i < MAX_RANGE; i++) {
            Row row = client.readRow(TableId.of(TestingUtils.TABLE), "key" + i);
            assertNull(row);
        }
    }

    /**
     * Helper method to verify that the data has been correctly written to the Bigtable table.
     *
     * @throws IOException If an error occurs while reading data from the table.
     * @throws InterruptedException If the thread is interrupted while waiting for the read
     *     operation.
     * @throws ExecutionException If an error occurs during the read operation.
     */
    private void checkWrittenData() {
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
    }

    /**
     * Helper method to convert a {@link ByteString} to an integer.
     *
     * @param byteString The {@link ByteString} to convert.
     * @return The converted integer value.
     */
    private int bytesToInteger(ByteString byteString) {
        return ByteBuffer.wrap(byteString.toByteArray()).getInt();
    }
}
