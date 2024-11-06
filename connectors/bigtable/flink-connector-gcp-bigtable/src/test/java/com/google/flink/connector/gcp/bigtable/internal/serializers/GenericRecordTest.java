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

package com.google.flink.connector.gcp.bigtable.internal.serializers;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.flink.connector.gcp.bigtable.testingutils.TestingUtils;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for the {@link GenericRecordToRowMutationSerializer} class.
 *
 * <p>This class verifies the functionality of the {@link GenericRecordToRowMutationSerializer} by
 * testing its ability to serialize {@link GenericRecord} objects into {@link RowMutationEntry}
 * objects, with and without nested fields. It also includes tests for data type conversions and
 * error handling.
 */
public class GenericRecordTest {

    /** Tests the serialization of a {@link GenericRecord} into a {@link RowMutationEntry}. */
    @Test
    public void testRowMutationSerialization() {
        testSerialization();
    }

    /**
     * Tests the conversion of various data types from a {@link GenericRecord} to byte arrays and
     * back.
     */
    @Test
    public void testRecordToBytes() {
        Schema schema =
                SchemaBuilder.record("TestBytes")
                        .fields()
                        .requiredString("stringField")
                        .requiredInt("intField")
                        .requiredLong("longField")
                        .requiredDouble("doubleField")
                        .requiredFloat("floatField")
                        .requiredBytes("bytesField")
                        .requiredBoolean("booleanField")
                        .endRecord();

        GenericRecord record = new GenericData.Record(schema);

        record.put("stringField", "Testing");
        record.put("intField", 1729);
        record.put("longField", 456789L);
        record.put("doubleField", 3.14159);
        record.put("floatField", 2.718f);
        record.put("bytesField", ByteBuffer.wrap("some bytes".getBytes()));
        record.put("booleanField", true);

        for (Schema.Field field : record.getSchema().getFields()) {
            byte[] convertedBytes =
                    GenericRecordToRowMutationSerializer.convertFieldToBytes(
                            record.get(field.name()), field.schema().getType());
            assertEquals(
                    record.get(field.name()),
                    convertBytesToField(convertedBytes, field.schema().getType()));
        }
    }

    /**
     * Creates a {@link GenericRecordToRowMutationSerializer} instance for testing.
     *
     * @return A configured {@link GenericRecordToRowMutationSerializer} instance.
     */
    private GenericRecordToRowMutationSerializer createTestSerializer() {
        GenericRecordToRowMutationSerializer.Builder builder =
                GenericRecordToRowMutationSerializer.builder()
                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD)
                        .withColumnFamily(TestingUtils.COLUMN_FAMILY);

        return builder.build();
    }

    /**
     * Creates a test {@link GenericRecord} with basic fields.
     *
     * @return A {@link GenericRecord} for testing serialization.
     */
    private static GenericRecord getTestGenericRecord() {
        Schema schema =
                SchemaBuilder.builder()
                        .record("Test")
                        .fields()
                        .requiredString(TestingUtils.ROW_KEY_FIELD)
                        .requiredString(TestingUtils.STRING_FIELD)
                        .requiredInt(TestingUtils.INTEGER_FIELD)
                        .endRecord();

        GenericRecord testRecord = new GenericData.Record(schema);

        testRecord.put(TestingUtils.ROW_KEY_FIELD, TestingUtils.ROW_KEY_VALUE);
        testRecord.put(TestingUtils.STRING_FIELD, TestingUtils.STRING_VALUE);
        testRecord.put(TestingUtils.INTEGER_FIELD, TestingUtils.INTEGER_VALUE);

        return testRecord;
    }

    /** Helper method to perform the serialization test with or without nested fields. */
    private void testSerialization() {
        GenericRecordToRowMutationSerializer serializer = createTestSerializer();
        RowMutationEntry wantedEntry = TestingUtils.getTestRowMutationEntry();

        GenericRecord record = getTestGenericRecord();
        RowMutationEntry serializedEntry = serializer.serialize(record, null);

        TestingUtils.assertRowMutationEntryEquality(serializedEntry, wantedEntry);
    }

    /**
     * Helper method to convert a byte array back to its corresponding data type based on the
     * provided {@link Schema.Type}.
     *
     * @param bytes The byte array to convert.
     * @param type The {@link Schema.Type} of the data.
     * @return The converted object.
     */
    private static Object convertBytesToField(byte[] bytes, Schema.Type type) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        switch (type) {
            case STRING:
                return new String(bytes);
            case BYTES:
                return ByteBuffer.wrap(bytes);
            case INT:
                return buffer.getInt();
            case LONG:
                return buffer.getLong();
            case FLOAT:
                return buffer.getFloat();
            case DOUBLE:
                return buffer.getDouble();
            case BOOLEAN:
                return buffer.get() != 0;
            default:
                throw new IllegalArgumentException("Unsupported Avro type: " + type);
        }
    }
}
