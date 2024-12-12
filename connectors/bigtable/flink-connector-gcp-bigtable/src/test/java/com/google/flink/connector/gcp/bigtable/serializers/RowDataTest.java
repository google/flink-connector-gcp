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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.flink.connector.gcp.bigtable.testingutils.TestingUtils;
import com.google.flink.connector.gcp.bigtable.utils.ErrorMessages;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for the {@link GenericRecordToRowMutationSerializer} class.
 *
 * <p>This class verifies the functionality of the {@link RowDataToRowMutationSerializer} by testing
 * its ability to serialize {@link RowData} objects into {@link RowMutationEntry} objects, with and
 * without nested fields. It also includes tests for data type conversions and error handling.
 */
public class RowDataTest {
    private final DataType dtSchema =
            DataTypes.ROW(
                    DataTypes.FIELD(TestingUtils.ROW_KEY_FIELD, DataTypes.STRING()),
                    DataTypes.FIELD(TestingUtils.STRING_FIELD, DataTypes.STRING()),
                    DataTypes.FIELD(TestingUtils.INTEGER_FIELD, DataTypes.INT()));

    private final DataType dtNestedSchema =
            DataTypes.ROW(
                    DataTypes.FIELD(TestingUtils.ROW_KEY_FIELD, DataTypes.STRING()),
                    DataTypes.FIELD(
                            TestingUtils.NESTED_COLUMN_FAMILY_1,
                            DataTypes.ROW(
                                    DataTypes.FIELD(TestingUtils.STRING_FIELD, DataTypes.STRING()),
                                    DataTypes.FIELD(TestingUtils.INTEGER_FIELD, DataTypes.INT()))),
                    DataTypes.FIELD(
                            TestingUtils.NESTED_COLUMN_FAMILY_2,
                            DataTypes.ROW(
                                    DataTypes.FIELD(
                                            TestingUtils.STRING_FIELD_2, DataTypes.STRING()))));

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCorrectSerializerInitialization(Boolean useNestedRowsMode) {
        RowDataToRowMutationSerializer.Builder builder =
                RowDataToRowMutationSerializer.builder()
                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD);

        if (useNestedRowsMode) {
            builder.withNestedRowsMode().withSchema(dtNestedSchema);
        } else {
            builder.withColumnFamily(TestingUtils.COLUMN_FAMILY).withSchema(dtSchema);
        }
        RowDataToRowMutationSerializer serializer = builder.build();

        assertEquals(TestingUtils.ROW_KEY_FIELD, serializer.rowKeyField);
        assertEquals(0, serializer.rowKeyIndex);
        assertEquals(
                useNestedRowsMode ? null : TestingUtils.COLUMN_FAMILY, serializer.columnFamily);
        assertEquals(useNestedRowsMode, serializer.useNestedRowsMode);
    }

    @Test
    public void testNullKeySerializerInitialization() {
        Assertions.assertThatThrownBy(
                        () ->
                                RowDataToRowMutationSerializer.builder()
                                        .withRowKeyField(null)
                                        .withColumnFamily(TestingUtils.COLUMN_FAMILY)
                                        .withSchema(dtSchema)
                                        .build())
                .hasMessage(ErrorMessages.ROW_KEY_FIELD_NULL);
    }

    @Test
    public void testNullSchemaSerializerInitialization() {
        Assertions.assertThatThrownBy(
                        () ->
                                RowDataToRowMutationSerializer.builder()
                                        .withSchema(null)
                                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD)
                                        .withColumnFamily(TestingUtils.COLUMN_FAMILY)
                                        .build())
                .hasMessage(ErrorMessages.ROW_KEY_FIELD_NULL);
    }

    @Test
    public void testColumnFamilyAndNestedIncompability() {
        Assertions.assertThatThrownBy(
                        () ->
                                RowDataToRowMutationSerializer.builder()
                                        .withSchema(dtSchema)
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
                                RowDataToRowMutationSerializer.builder()
                                        .withSchema(dtSchema)
                                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD)
                                        .build())
                .hasMessageContaining(ErrorMessages.COLUMN_FAMILY_OR_NESTED_ROWS_REQUIRED);
    }

    @Test
    public void testWrongSchemaNestedRowsMode() {
        Assertions.assertThatThrownBy(
                        () ->
                                RowDataToRowMutationSerializer.builder()
                                        .withSchema(dtSchema)
                                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD)
                                        .withNestedRowsMode()
                                        .build())
                .hasMessageContaining(ErrorMessages.BASE_NO_NESTED_TYPE + "ROW");
    }

    @Test
    public void testDoubleNestedRowsError() {
        DataType innerNest =
                DataTypes.ROW(DataTypes.FIELD(TestingUtils.STRING_FIELD_2, DataTypes.STRING()));

        DataType outerNest =
                DataTypes.ROW(
                        DataTypes.FIELD(TestingUtils.STRING_FIELD, DataTypes.STRING()),
                        DataTypes.FIELD("doubleNest", innerNest));

        DataType dtDoubleNestedSchema =
                DataTypes.ROW(
                        DataTypes.FIELD(TestingUtils.ROW_KEY_FIELD, DataTypes.STRING()),
                        DataTypes.FIELD(TestingUtils.NESTED_COLUMN_FAMILY_1, outerNest));

        Assertions.assertThatThrownBy(
                        () ->
                                RowDataToRowMutationSerializer.builder()
                                        .withSchema(dtDoubleNestedSchema)
                                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD)
                                        .withNestedRowsMode()
                                        .build())
                .hasMessageContaining(ErrorMessages.NESTED_TYPE_ERROR);
    }

    @Test
    public void testTimestampPrecisionError() {
        DataType dtTimestampSchema =
                DataTypes.ROW(
                        DataTypes.FIELD(TestingUtils.ROW_KEY_FIELD, DataTypes.STRING()),
                        DataTypes.FIELD(
                                "timestamp",
                                DataTypes.TIMESTAMP(
                                        RowDataToRowMutationSerializer.MAX_DATETIME_PRECISION
                                                + 1)));
        Assertions.assertThatThrownBy(
                        () ->
                                RowDataToRowMutationSerializer.builder()
                                        .withSchema(dtTimestampSchema)
                                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD)
                                        .withColumnFamily(TestingUtils.COLUMN_FAMILY)
                                        .build())
                .hasMessageContaining(
                        String.format(
                                ErrorMessages.TIMESTAMP_OUTSIDE_PRECISION_TEMPLATE,
                                DataTypes.TIMESTAMP(
                                        RowDataToRowMutationSerializer.MAX_DATETIME_PRECISION + 1),
                                RowDataToRowMutationSerializer.MIN_DATETIME_PRECISION,
                                RowDataToRowMutationSerializer.MAX_DATETIME_PRECISION));
    }

    @Test
    public void testRowKeyNoStringError() {
        DataType dtIntegerKeySchema =
                DataTypes.ROW(DataTypes.FIELD(TestingUtils.ROW_KEY_FIELD, DataTypes.INT()));

        Assertions.assertThatThrownBy(
                        () ->
                                RowDataToRowMutationSerializer.builder()
                                        .withSchema(dtIntegerKeySchema)
                                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD)
                                        .withColumnFamily(TestingUtils.COLUMN_FAMILY)
                                        .build())
                .hasMessage(
                        String.format(ErrorMessages.ROW_KEY_STRING_TYPE_TEMPLATE, DataTypes.INT()));
    }

    @Test
    public void testMissingRowKey() {
        DataType dtIntegerKeySchema =
                DataTypes.ROW(DataTypes.FIELD(TestingUtils.STRING_FIELD, DataTypes.STRING()));

        Assertions.assertThatThrownBy(
                        () ->
                                RowDataToRowMutationSerializer.builder()
                                        .withSchema(dtIntegerKeySchema)
                                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD)
                                        .withColumnFamily(TestingUtils.COLUMN_FAMILY)
                                        .build())
                .hasMessageContaining(
                        String.format(
                                ErrorMessages.MISSING_ROW_KEY_TEMPLATE,
                                TestingUtils.ROW_KEY_FIELD,
                                dtIntegerKeySchema));
    }

    @Test
    public void testTimeNestedPrecisionError() {
        DataType dtTimeNestedSchema =
                DataTypes.ROW(
                        DataTypes.FIELD(TestingUtils.ROW_KEY_FIELD, DataTypes.STRING()),
                        DataTypes.FIELD(
                                TestingUtils.NESTED_COLUMN_FAMILY_1,
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "time",
                                                DataTypes.TIME(
                                                        RowDataToRowMutationSerializer
                                                                        .MAX_DATETIME_PRECISION
                                                                + 1)))));
        Assertions.assertThatThrownBy(
                        () ->
                                RowDataToRowMutationSerializer.builder()
                                        .withSchema(dtTimeNestedSchema)
                                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD)
                                        .withNestedRowsMode()
                                        .build())
                .hasMessageContaining(
                        String.format(
                                ErrorMessages.TIMESTAMP_OUTSIDE_PRECISION_TEMPLATE,
                                DataTypes.TIME(
                                        RowDataToRowMutationSerializer.MAX_DATETIME_PRECISION + 1),
                                RowDataToRowMutationSerializer.MIN_DATETIME_PRECISION,
                                RowDataToRowMutationSerializer.MAX_DATETIME_PRECISION));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRowMutationSerialization(Boolean useNestedRowsMode) {
        RowDataToRowMutationSerializer serializer = createTestSerializer(useNestedRowsMode);
        RowMutationEntry wantedEntry = TestingUtils.getTestRowMutationEntry(useNestedRowsMode);

        RowData row = getRowData(useNestedRowsMode);
        RowMutationEntry serializedEntry = serializer.serialize(row, null);
        TestingUtils.assertRowMutationEntryEquality(serializedEntry, wantedEntry);
    }

    @Test
    public void testRecordToBytes() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("stringField", DataTypes.STRING()),
                        DataTypes.FIELD("charField", DataTypes.CHAR(10)),
                        DataTypes.FIELD("varcharField", DataTypes.VARCHAR(50)),
                        DataTypes.FIELD("booleanField", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("tinyintField", DataTypes.TINYINT()),
                        DataTypes.FIELD("smallintField", DataTypes.SMALLINT()),
                        DataTypes.FIELD("integerField", DataTypes.INT()),
                        DataTypes.FIELD("longField", DataTypes.BIGINT()),
                        DataTypes.FIELD("doubleField", DataTypes.DOUBLE()),
                        DataTypes.FIELD("floatField", DataTypes.FLOAT()),
                        DataTypes.FIELD("bytesField", DataTypes.BYTES()),
                        DataTypes.FIELD("binaryField", DataTypes.BINARY(8)),
                        DataTypes.FIELD(
                                "timestampWithTimeZoneField",
                                DataTypes.TIMESTAMP_WITH_TIME_ZONE(1)),
                        DataTypes.FIELD("timestampWithoutTimeZoneField", DataTypes.TIMESTAMP(2)),
                        DataTypes.FIELD(
                                "timestampWithLocalTimeZoneField",
                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
                        DataTypes.FIELD(
                                "timestampWithoutTimeZoneFieldSecondsPrecision",
                                DataTypes.TIMESTAMP()),
                        DataTypes.FIELD(
                                "intervalYearMonthField",
                                DataTypes.INTERVAL(DataTypes.YEAR(), DataTypes.MONTH())),
                        DataTypes.FIELD(
                                "intervalDaysTimeField",
                                DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND())),
                        DataTypes.FIELD("timeWithoutTimeZoneField", DataTypes.TIME(3)),
                        DataTypes.FIELD(
                                "timeWithoutTimeZoneFieldSecondsPrecision", DataTypes.TIME()),
                        DataTypes.FIELD("dateField", DataTypes.DATE()),
                        DataTypes.FIELD("decimalField", DataTypes.DECIMAL(9, 5)));

        GenericRowData row = new GenericRowData(22);
        row.setField(0, StringData.fromString("string-value")); // stringField
        row.setField(1, StringData.fromString("char-value")); // charField
        row.setField(2, StringData.fromString("varchar-value")); // varcharField
        row.setField(3, true); // booleanField
        row.setField(4, (short) 1); // tinyintField
        row.setField(5, (short) 32767); // smallintField
        row.setField(6, 123456789); // integerField
        row.setField(7, 12345678987654321L); // longField
        row.setField(8, 3.14159); // doubleField
        row.setField(9, 1.234f); // floatField
        row.setField(10, "testing".getBytes()); // bytesField
        row.setField(11, ByteBuffer.allocate(8).putInt(1025).array()); // binaryField
        row.setField(
                12,
                TimestampData.fromInstant(
                        Instant.now(Clock.systemUTC()))); // timestampWithTimeZoneField
        row.setField(13, TimestampData.fromInstant(Instant.now())); // timestampWithoutTimeZoneField
        row.setField(
                14,
                TimestampData.fromLocalDateTime(
                        LocalDateTime.of(
                                2024, 1, 1, 12, 30, 0, 0))); // timestampWithLocalTimeZoneField
        row.setField(
                15,
                TimestampData.fromLocalDateTime(
                        LocalDateTime.of(
                                2024, 1, 1, 12, 30, 0,
                                15))); // timestampWithoutTimeZoneFieldSecondsPrecision
        row.setField(16, 13); // intervalYearMonthField, represented internally as int
        row.setField(17, 100L); // periodDays, represented internally as long
        row.setField(18, LocalTime.of(15, 45, 30).toSecondOfDay()); // timeWithoutTimeZoneField
        row.setField(
                19,
                LocalTime.of(15, 45, 0)
                        .toSecondOfDay()); // timeWithoutTimeZoneFieldSecondsPrecision
        row.setField(20, LocalDate.of(2024, 1, 15).toEpochDay()); // dateField
        row.setField(21, DecimalData.fromBigDecimal(new BigDecimal("1729.92710"), 9, 5));

        for (int i = 0; i < row.getArity(); i++) {
            LogicalType type = schema.getLogicalType().getChildren().get(i);
            byte[] convertedBytes =
                    RowDataToRowMutationSerializer.convertFieldToBytes(row, i, type);
            assertEquals(getObject(row, i, type), convertBytesToField(convertedBytes, type));
        }
    }

    private RowDataToRowMutationSerializer createTestSerializer(Boolean useNestedRows) {
        RowDataToRowMutationSerializer.Builder builder =
                RowDataToRowMutationSerializer.builder()
                        .withRowKeyField(TestingUtils.ROW_KEY_FIELD);
        if (useNestedRows) {
            builder.withNestedRowsMode().withSchema(dtNestedSchema);
        } else {
            builder.withColumnFamily(TestingUtils.COLUMN_FAMILY).withSchema(dtSchema);
        }
        return builder.build();
    }

    private static RowData getRowData(Boolean useNestedRows) {
        GenericRowData r = new GenericRowData(3);
        r.setField(0, StringData.fromString(TestingUtils.ROW_KEY_VALUE));
        if (useNestedRows) {
            GenericRowData nestedRow1 = new GenericRowData(2);
            nestedRow1.setField(0, StringData.fromString(TestingUtils.STRING_VALUE));
            nestedRow1.setField(1, TestingUtils.INTEGER_VALUE);

            GenericRowData nestedRow2 = new GenericRowData(1);
            nestedRow2.setField(0, StringData.fromString(TestingUtils.STRING_VALUE_2));

            r.setField(1, nestedRow1);
            r.setField(2, nestedRow2);
        } else {
            r.setField(1, StringData.fromString(TestingUtils.STRING_VALUE));
            r.setField(2, TestingUtils.INTEGER_VALUE);
        }
        return r;
    }

    private static Object getObject(RowData row, Integer index, LogicalType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return row.getString(index);
            case BOOLEAN:
                return row.getBoolean(index);
            case TINYINT:
            case SMALLINT:
                return row.getShort(index);
            case INTERVAL_YEAR_MONTH:
            case INTEGER:
                return row.getInt(index);
            case INTERVAL_DAY_TIME:
            case BIGINT:
                return row.getLong(index);
            case FLOAT:
                return row.getFloat(index);
            case DOUBLE:
                return row.getDouble(index);
            case VARBINARY:
            case BINARY:
                return row.getBinary(index);
            case DATE:
                return LocalDate.ofEpochDay(row.getLong(index));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int tsPrecision = RowDataToRowMutationSerializer.getPrecisionOr(type);
                return row.getTimestamp(index, tsPrecision);
            case TIME_WITHOUT_TIME_ZONE:
                int milliseconds = row.getInt(index);
                return LocalTime.ofNanoOfDay(
                        milliseconds * 1_000_000L); // Assuming you want milliseconds for TIME
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                int decimalPrecision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                return row.getDecimal(index, decimalPrecision, scale);
            default:
                throw new IllegalArgumentException("Unsupported data type: " + type);
        }
    }

    private static Object convertBytesToField(byte[] bytes, LogicalType type) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return StringData.fromBytes(bytes);
            case BOOLEAN:
                return buffer.get() != 0;
            case TINYINT:
            case SMALLINT:
                return buffer.getShort();
            case INTERVAL_YEAR_MONTH:
            case INTEGER:
                return buffer.getInt();
            case INTERVAL_DAY_TIME:
            case BIGINT:
                return buffer.getLong();
            case FLOAT:
                return buffer.getFloat();
            case DOUBLE:
                return buffer.getDouble();
            case VARBINARY:
            case BINARY:
                return bytes; // No conversion needed for binary types
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    long milliseconds = buffer.getLong(0);
                    int nanos = buffer.getInt(Long.BYTES);
                    return TimestampData.fromEpochMillis(milliseconds, nanos);
                }
            case TIME_WITHOUT_TIME_ZONE:
                int milliseconds = buffer.getInt();
                return LocalTime.ofNanoOfDay(milliseconds * 1_000_000L);
            case DATE:
                long daysSinceEpoch = buffer.getLong();
                return LocalDate.ofEpochDay(daysSinceEpoch);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                return DecimalData.fromUnscaledBytes(bytes, precision, scale);
            default:
                throw new IllegalArgumentException("Unsupported data type: " + type);
        }
    }
}
