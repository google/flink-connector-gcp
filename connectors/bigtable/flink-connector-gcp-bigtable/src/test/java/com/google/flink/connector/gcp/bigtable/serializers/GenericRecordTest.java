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

package com.google.flink.connector.gcp.bigtable.serializers;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.flink.connector.gcp.bigtable.testingutils.TestingUtils;
import com.google.flink.connector.gcp.bigtable.utils.ErrorMessages;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCorrectSerializerInitialization(Boolean useNestedRowsMode) {
        GenericRecordToRowMutationSerializer.Builder builder =
                GenericRecordToRowMutationSerializer.builder()
                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD);
        if (useNestedRowsMode) {
            builder.withNestedRowsMode();
        } else {
            builder.withColumnFamily(TestingUtils.COLUMN_FAMILY);
        }
        GenericRecordToRowMutationSerializer serializer = builder.build();

        assertEquals(TestingUtils.ROW_KEY_FIELD, serializer.rowKeyField);
        assertEquals(
                useNestedRowsMode ? null : TestingUtils.COLUMN_FAMILY, serializer.columnFamily);
        assertEquals(useNestedRowsMode, serializer.useNestedRowsMode);
    }

    @Test
    public void testColumnFamilyAndNestedIncompability() {
        Assertions.assertThatThrownBy(
                        () ->
                                GenericRecordToRowMutationSerializer.builder()
                                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD)
                                        .withNestedRowsMode()
                                        .withColumnFamily(TestingUtils.COLUMN_FAMILY)
                                        .build())
                .hasMessageContaining(ErrorMessages.COLUMN_FAMILY_AND_NESTED_INCOMPATIBLE);
    }

    @Test
    public void testNotColumnFamilyOrNestedRequired() {
        Assertions.assertThatThrownBy(
                        () ->
                                GenericRecordToRowMutationSerializer.builder()
                                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD)
                                        .build())
                .hasMessageContaining(ErrorMessages.COLUMN_FAMILY_OR_NESTED_ROWS_REQUIRED);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCorrectRowMutationSerialization(Boolean useNestedRowsMode) {
        GenericRecordToRowMutationSerializer serializer = createTestSerializer(useNestedRowsMode);
        RowMutationEntry wantedEntry = TestingUtils.getTestRowMutationEntry(useNestedRowsMode);

        GenericRecord record =
                useNestedRowsMode ? getTestGenericRecordNested() : getTestGenericRecord();
        RowMutationEntry serializedEntry = serializer.serialize(record, null);

        TestingUtils.assertRowMutationEntryEquality(serializedEntry, wantedEntry);
    }

    @Test
    public void testWrongSchemaNestedRowsMode() {
        GenericRecordToRowMutationSerializer serializer = createTestSerializer(true);

        GenericRecord record = getTestGenericRecord();

        Assertions.assertThatThrownBy(() -> serializer.serialize(record, null))
                .hasMessageContaining(ErrorMessages.BASE_NO_NESTED_TYPE);
    }

    @Test
    public void testDoubleNestedRowsError() {
        GenericRecordToRowMutationSerializer serializer = createTestSerializer(true);

        GenericRecord testRecord = getTestGenericRecordDoubleNested();

        Assertions.assertThatThrownBy(() -> serializer.serialize(testRecord, null))
                .hasMessageContaining(ErrorMessages.NESTED_TYPE_ERROR);
    }

    @Test
    public void testRowKeyNoStringError() {
        GenericRecordToRowMutationSerializer serializer = createTestSerializer(false);

        Schema schema =
                SchemaBuilder.record("TestBytes")
                        .fields()
                        .requiredInt(TestingUtils.ROW_KEY_FIELD)
                        .endRecord();

        GenericRecord testRecord = new GenericData.Record(schema);
        testRecord.put(TestingUtils.ROW_KEY_FIELD, 1234);

        Assertions.assertThatThrownBy(() -> serializer.serialize(testRecord, null))
                .hasMessageContaining(ErrorMessages.ROW_KEY_STRING_TYPE);
    }

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

    private GenericRecordToRowMutationSerializer createTestSerializer(Boolean useNestedRowsMode) {
        GenericRecordToRowMutationSerializer.Builder builder =
                GenericRecordToRowMutationSerializer.builder()
                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD);
        if (useNestedRowsMode) {
            builder.withNestedRowsMode();
        } else {
            builder.withColumnFamily(TestingUtils.COLUMN_FAMILY);
        }
        return builder.build();
    }

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

    private static GenericRecord getTestGenericRecordNested() {
        Schema nestedSchema1 =
                SchemaBuilder.builder()
                        .record(TestingUtils.NESTED_COLUMN_FAMILY_1)
                        .fields()
                        .requiredString(TestingUtils.STRING_FIELD)
                        .requiredInt(TestingUtils.INTEGER_FIELD)
                        .endRecord();

        Schema nestedSchema2 =
                SchemaBuilder.builder()
                        .record(TestingUtils.NESTED_COLUMN_FAMILY_2)
                        .fields()
                        .requiredString(TestingUtils.STRING_FIELD_2)
                        .endRecord();

        Schema schema =
                SchemaBuilder.builder()
                        .record("Test")
                        .fields()
                        .requiredString(TestingUtils.ROW_KEY_FIELD)
                        .name(TestingUtils.NESTED_COLUMN_FAMILY_1)
                        .type(nestedSchema1)
                        .noDefault()
                        .name(TestingUtils.NESTED_COLUMN_FAMILY_2)
                        .type(nestedSchema2)
                        .noDefault()
                        .endRecord();

        GenericRecord testRecord = new GenericData.Record(schema);
        GenericRecord nestedRecord1 = new GenericData.Record(nestedSchema1);
        GenericRecord nestedRecord2 = new GenericData.Record(nestedSchema2);

        nestedRecord1.put(TestingUtils.STRING_FIELD, TestingUtils.STRING_VALUE);
        nestedRecord1.put(TestingUtils.INTEGER_FIELD, TestingUtils.INTEGER_VALUE);

        nestedRecord2.put(TestingUtils.STRING_FIELD_2, TestingUtils.STRING_VALUE_2);

        testRecord.put(TestingUtils.ROW_KEY_FIELD, TestingUtils.ROW_KEY_VALUE);
        testRecord.put(TestingUtils.NESTED_COLUMN_FAMILY_1, nestedRecord1);
        testRecord.put(TestingUtils.NESTED_COLUMN_FAMILY_2, nestedRecord2);

        return testRecord;
    }

    private static GenericRecord getTestGenericRecordDoubleNested() {
        Schema doubleNestSchema =
                SchemaBuilder.builder()
                        .record("doubleNest")
                        .fields()
                        .requiredString(TestingUtils.STRING_FIELD)
                        .endRecord();
        Schema nestSchema =
                SchemaBuilder.builder()
                        .record("nest")
                        .fields()
                        .name("doubleNest")
                        .type(doubleNestSchema)
                        .noDefault()
                        .endRecord();
        Schema schema =
                SchemaBuilder.builder()
                        .record("Test")
                        .fields()
                        .requiredString(TestingUtils.ROW_KEY_FIELD)
                        .name("nest")
                        .type(nestSchema)
                        .noDefault()
                        .endRecord();

        GenericRecord testRecord = new GenericData.Record(schema);
        GenericRecord nest = new GenericData.Record(nestSchema);
        GenericRecord doubleNest = new GenericData.Record(doubleNestSchema);

        doubleNest.put(TestingUtils.STRING_FIELD, TestingUtils.STRING_VALUE);
        nest.put("doubleNest", doubleNest);

        testRecord.put(TestingUtils.ROW_KEY_FIELD, TestingUtils.ROW_KEY_VALUE);
        testRecord.put("nest", nest);
        return testRecord;
    }

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
