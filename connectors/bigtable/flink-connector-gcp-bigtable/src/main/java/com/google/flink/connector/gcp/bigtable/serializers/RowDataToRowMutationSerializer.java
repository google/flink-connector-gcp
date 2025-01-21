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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.flink.connector.gcp.bigtable.utils.BigtableUtils;
import com.google.flink.connector.gcp.bigtable.utils.ErrorMessages;
import com.google.protobuf.ByteString;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializer that converts {@link RowData} objects into Bigtable {@link RowMutationEntry}
 * objects.
 *
 * <p>This serializer supports two modes of operation:
 *
 * <ol>
 *   <li><b>Column Family Mode:</b> All fields in the {@link RowData} are written to a single
 *       specified column family. Nested fields are not supported in this mode.
 *   <li><b>Nested Rows Mode:</b> Each field in the {@link RowData} (except the row key field)
 *       represents a separate column family, and its value must be another {@link RowData}
 *       containing the columns for that family. Only single nested rows are supported.
 * </ol>
 *
 * <p>The serialization process involves extracting data from the {@link RowData} fields and
 * converting them to appropriate byte arrays for Bigtable.
 */
public class RowDataToRowMutationSerializer implements BaseRowMutationSerializer<RowData> {
    public final String columnFamily;
    public Boolean useNestedRowsMode;
    public String rowKeyField;
    public Integer rowKeyIndex;

    private final HashMap<String, DataType> columnTypeMap = new HashMap<String, DataType>();
    private final HashMap<Integer, String> indexMap = new HashMap<Integer, String>();
    private final HashMap<String, HashMap<Integer, String>> nestedIndexMap =
            new HashMap<String, HashMap<Integer, String>>();

    protected static final int MIN_DATETIME_PRECISION = 0;
    protected static final int MAX_DATETIME_PRECISION = 6;

    /**
     * Constructs a {@code RowDataToMutationSerializer}.
     *
     * @param schema The {@link DataType} of the {@link RowData} to be serialized.
     * @param rowKeyField The name of the field in the {@link RowData} that represents the Bigtable
     *     <a href="https://cloud.google.com/bigtable/docs/schema-design#row-keys">row key</a>. It
     *     must be of type String.
     * @param useNestedRowsMode Whether to use nested rows mode. If {@code true}, each field in the
     *     {@link RowData} (except the row key field) represents a separate column family.
     *     Otherwise, all fields are written to a single column family specified by {@code
     *     columnFamily}.
     * @param columnFamily The name of the <a
     *     href="https://cloud.google.com/bigtable/docs/schema-design#column-families">column
     *     family</a> to use (only in column family mode).
     */
    public RowDataToRowMutationSerializer(
            DataType schema,
            String rowKeyField,
            Boolean useNestedRowsMode,
            @Nullable String columnFamily) {
        this.columnFamily = columnFamily;
        this.useNestedRowsMode = useNestedRowsMode;
        this.rowKeyField = rowKeyField;

        // Go through schema to generate maps
        generateMapsFromSchema(schema, rowKeyField);

        checkNotNull(
                this.rowKeyIndex,
                String.format(
                        ErrorMessages.MISSING_ROW_KEY_TEMPLATE, rowKeyField, schema.toString()));
    }

    @Override
    public RowMutationEntry serialize(RowData record, SinkWriter.Context context) {
        RowMutationEntry entry =
                RowMutationEntry.create(record.getString(this.rowKeyIndex).toString());

        if (!useNestedRowsMode) {
            return serializeWithColumnFamily(
                    record, entry, this.columnFamily, this.indexMap, context);
        }

        for (int i = 0; i < record.getArity(); i++) {
            if (i != this.rowKeyIndex) {
                String columnFamilyName = this.indexMap.get(i);
                RowData nestedRow =
                        record.getRow(
                                i, this.columnTypeMap.get(columnFamilyName).getChildren().size());
                serializeWithColumnFamily(
                        nestedRow,
                        entry,
                        columnFamilyName,
                        this.nestedIndexMap.get(columnFamilyName),
                        context);
            }
        }
        return entry;
    }

    private RowMutationEntry serializeWithColumnFamily(
            RowData record,
            RowMutationEntry entry,
            String columnFamily,
            HashMap<Integer, String> innerMap,
            SinkWriter.Context context) {
        for (int i = 0; i < record.getArity(); i++) {
            // Skip serializing the rowKey
            Boolean isRowKey = (innerMap == this.indexMap) && (i == this.rowKeyIndex);
            if (!isRowKey) {
                String column = innerMap.get(i);
                byte[] valueBytes =
                        convertFieldToBytes(record, i, columnTypeMap.get(column).getLogicalType());
                entry.setCell(
                        columnFamily,
                        ByteString.copyFromUtf8(column),
                        BigtableUtils.getTimestamp(context),
                        ByteString.copyFrom(valueBytes));
            }
        }
        return entry;
    }

    /**
     * Generates maps from the schema to keep track of DataTypes and indexes. This is needed since
     * the schema cannot be fetched from the {@link RowData}.
     *
     * @param schema The {@link DataType} of the {@link RowData}.
     * @param rowKeyField The name of the row key field.
     */
    private void generateMapsFromSchema(DataType schema, String rowKeyField) {
        int index = 0;
        for (Field field : DataType.getFields(schema)) {
            // Check if key
            if (field.getName().equals(rowKeyField)) {
                if (!field.getDataType().getLogicalType().is(LogicalTypeFamily.CHARACTER_STRING)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    ErrorMessages.ROW_KEY_STRING_TYPE_TEMPLATE,
                                    field.getDataType()));
                }
                this.rowKeyIndex = index;
            }

            // Populate maps for ROWs
            if (field.getDataType().getLogicalType().is(LogicalTypeRoot.ROW)) {
                if (!useNestedRowsMode) {
                    throw new IllegalArgumentException(ErrorMessages.NESTED_TYPE_ERROR);
                }
                int nestedIndex = 0;
                HashMap<Integer, String> nestedIndexes = new HashMap<Integer, String>();
                for (Field nestedField : DataType.getFields(field.getDataType())) {
                    // Verify precision for DATETIME types
                    if (nestedField.getDataType().getLogicalType().is(LogicalTypeFamily.DATETIME)) {
                        getPrecisionOr(nestedField.getDataType().getLogicalType());
                    }

                    if (nestedField.getDataType().getLogicalType().is(LogicalTypeRoot.ROW)) {
                        throw new IllegalArgumentException(ErrorMessages.NESTED_TYPE_ERROR);
                    }

                    nestedIndexes.put(nestedIndex, nestedField.getName());
                    columnTypeMap.put(nestedField.getName(), nestedField.getDataType());
                    nestedIndex++;
                }
                nestedIndexMap.put(field.getName(), nestedIndexes);
            }

            // Verify only non-row type in `nestedRowsModed` is the key
            Boolean isNotRowAndNotKey =
                    this.useNestedRowsMode
                            && !field.getDataType().getLogicalType().is(LogicalTypeRoot.ROW)
                            && !field.getName().equals(rowKeyField);
            if (isNotRowAndNotKey) {
                throw new IllegalArgumentException(ErrorMessages.BASE_NO_NESTED_TYPE + "ROW");
            }

            // Verify precision for DATETIME types
            if (field.getDataType().getLogicalType().is(LogicalTypeFamily.DATETIME)) {
                getPrecisionOr(field.getDataType().getLogicalType());
            }

            columnTypeMap.put(field.getName(), field.getDataType());
            indexMap.put(index, field.getName());
            index++;
        }
    }

    /**
     * Converts a field in a {@link RowData} to a byte array.
     *
     * @param row The {@link RowData} containing the field.
     * @param index The index of the field in the {@link RowData}.
     * @param type The {@link LogicalType} of the field.
     * @return The byte array representation of the field.
     */
    @VisibleForTesting
    static byte[] convertFieldToBytes(RowData row, Integer index, LogicalType type) {
        // Use the DataType to determine the appropriate conversion
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return row.getString(index).toBytes();
            case BOOLEAN:
                return ByteBuffer.allocate(1)
                        .put(row.getBoolean(index) ? (byte) 1 : (byte) 0)
                        .array();
            case TINYINT:
            case SMALLINT:
                return ByteBuffer.allocate(Short.BYTES).putShort(row.getShort(index)).array();
            case INTERVAL_YEAR_MONTH:
            case INTEGER:
                return ByteBuffer.allocate(Integer.BYTES).putInt(row.getInt(index)).array();
            case DATE:
            case INTERVAL_DAY_TIME:
            case BIGINT:
                return ByteBuffer.allocate(Long.BYTES).putLong(row.getLong(index)).array();
            case FLOAT:
                return ByteBuffer.allocate(Float.BYTES).putFloat(row.getFloat(index)).array();
            case DOUBLE:
                return ByteBuffer.allocate(Double.BYTES).putDouble(row.getDouble(index)).array();
            case VARBINARY:
            case BINARY:
                return row.getBinary(index);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int tsPrecision = getPrecisionOr(type);
                TimestampData ts = row.getTimestamp(index, tsPrecision);
                ByteBuffer buffer =
                        ByteBuffer.allocate(
                                Long.BYTES + Integer.BYTES); // 8 bytes for long + 4 bytes for
                buffer.putLong(ts.getMillisecond());
                buffer.putInt(ts.getNanoOfMillisecond());
                return buffer.array();
            case TIME_WITHOUT_TIME_ZONE:
                getPrecisionOr(type);
                return ByteBuffer.allocate(Integer.BYTES).putInt(row.getInt(index)).array();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                return row.getDecimal(index, precision, scale).toUnscaledBytes();
            case ROW:
                throw new IllegalArgumentException(ErrorMessages.NESTED_TYPE_ERROR);
            default:
                throw new IllegalArgumentException(
                        ErrorMessages.UNSUPPORTED_SERIALIZATION_TYPE + type.getTypeRoot());
        }
    }

    /**
     * Converts a byte array to a field value based on its {@link LogicalType}.
     *
     * @param bytes The byte array to convert.
     * @param type The {@link LogicalType} of the field.
     * @return The field value converted from the byte array.
     * @throws IllegalArgumentException If the Avro logical type is not supported.
     */
    public static Object convertBytesToField(byte[] bytes, LogicalType type) {
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

    @VisibleForTesting
    static int getPrecisionOr(LogicalType type) {
        int precision = getPrecision(type);
        if (precision < MIN_DATETIME_PRECISION || precision > MAX_DATETIME_PRECISION) {
            throw new IllegalArgumentException(
                    String.format(
                            ErrorMessages.TIMESTAMP_OUTSIDE_PRECISION_TEMPLATE,
                            type,
                            MIN_DATETIME_PRECISION,
                            MAX_DATETIME_PRECISION,
                            precision));
        }
        return precision;
    }

    /**
     * Creates a new {@link Builder} for constructing {@code RowDataToMutationSerializer} instances.
     *
     * @return A new {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** A builder class for creating {@code RowDataToMutationSerializer} instances. */
    public static class Builder {
        private DataType schema;
        private String rowKeyField;
        private String columnFamily;
        private Boolean useNestedRowsMode = false;

        public Builder withSchema(DataType schema) {
            this.schema = schema;
            return this;
        }

        public Builder withRowKeyField(String rowKeyField) {
            this.rowKeyField = rowKeyField;
            return this;
        }

        public Builder withColumnFamily(String columnFamily) {
            this.columnFamily = columnFamily;
            return this;
        }

        public Builder withNestedRowsMode() {
            useNestedRowsMode = true;
            return this;
        }

        public RowDataToRowMutationSerializer build() {
            checkNotNull(this.rowKeyField, ErrorMessages.ROW_KEY_FIELD_NULL);
            checkNotNull(this.schema, ErrorMessages.SCHEMA_NULL);
            if (columnFamily != null && useNestedRowsMode) {
                throw new IllegalArgumentException(
                        ErrorMessages.COLUMN_FAMILY_AND_NESTED_INCOMPATIBLE);
            } else if (columnFamily == null && !useNestedRowsMode) {
                throw new IllegalArgumentException(
                        ErrorMessages.COLUMN_FAMILY_OR_NESTED_ROWS_REQUIRED);
            }
            return new RowDataToRowMutationSerializer(
                    schema, rowKeyField, useNestedRowsMode, columnFamily);
        }
    }
}
