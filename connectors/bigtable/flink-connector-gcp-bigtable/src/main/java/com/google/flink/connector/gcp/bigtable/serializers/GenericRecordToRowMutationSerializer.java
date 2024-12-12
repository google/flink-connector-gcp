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

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.flink.connector.gcp.bigtable.utils.BigtableUtils;
import com.google.flink.connector.gcp.bigtable.utils.ErrorMessages;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializer that converts {@link GenericRecord} objects into Bigtable {@link RowMutationEntry}
 * objects.
 *
 * <p>This serializer supports two modes of operation:
 *
 * <ol>
 *   <li>**Column Family Mode:** All fields in the {@link GenericRecord} are written to a single
 *       specified column family. Nested fields are not supported in this mode.
 *   <li>**Nested Rows Mode:** Each field in the {@link GenericRecord} (except the row key field)
 *       represents a separate column family, and its value must be another {@link GenericRecord}
 *       containing the columns for that family. Only single nested rows are supported.
 * </ol>
 *
 * <p>The serialization process involves extracting data from the {@link GenericRecord} fields and
 * converting them to appropriate byte arrays for Bigtable.
 */
public class GenericRecordToRowMutationSerializer
        implements BaseRowMutationSerializer<GenericRecord> {
    public final String rowKeyField;
    public final @Nullable String columnFamily;
    public final Boolean useNestedRowsMode;

    /**
     * Constructs a {@code GenericRecordToRowMutationSerializer}.
     *
     * @param rowKeyField The name of the field in the {@link GenericRecord} that represents the
     *     Bigtable <a href="https://cloud.google.com/bigtable/docs/schema-design#row-keys">row
     *     key</a>. It must be of type String.
     * @param useNestedRowsMode Whether to use nested rows mode. If {@code true}, each field in the
     *     {@link GenericRecord} (except the row key field) represents a separate column family.
     *     Otherwise, all fields are written to a single column family specified by {@code
     *     columnFamily}.
     * @param columnFamily The name of the <a
     *     href="https://cloud.google.com/bigtable/docs/schema-design#column-families">column
     *     family</a> to use (only in column family mode).
     */
    public GenericRecordToRowMutationSerializer(
            String rowKeyField, Boolean useNestedRowsMode, @Nullable String columnFamily) {
        this.rowKeyField = rowKeyField;
        this.columnFamily = columnFamily;
        this.useNestedRowsMode = useNestedRowsMode;
    }

    @Override
    public RowMutationEntry serialize(GenericRecord record, SinkWriter.Context context) {
        if (record.getSchema().getField(rowKeyField).schema().getType() != Schema.Type.STRING) {
            throw new RuntimeException(
                    String.format(
                            ErrorMessages.ROW_KEY_STRING_TYPE_TEMPLATE,
                            record.getSchema().getField(rowKeyField).schema().getType()));
        }

        RowMutationEntry entry = RowMutationEntry.create((String) record.get(rowKeyField));

        if (!useNestedRowsMode) {
            return serializeWithColumnFamily(record, entry, this.columnFamily, context);
        }

        for (Schema.Field field : record.getSchema().getFields()) {
            String columnFamily = field.name();
            if (!columnFamily.equals(rowKeyField)) {
                if (field.schema().getType() != Schema.Type.RECORD) {
                    throw new RuntimeException(ErrorMessages.BASE_NO_NESTED_TYPE + "RECORD");
                }
                GenericRecord nestedRecord = (GenericRecord) record.get(columnFamily);
                serializeWithColumnFamily(nestedRecord, entry, columnFamily, context);
            }
        }
        return entry;
    }

    private RowMutationEntry serializeWithColumnFamily(
            GenericRecord record,
            RowMutationEntry entry,
            String columnFamilyName,
            SinkWriter.Context context) {
        for (Schema.Field field : record.getSchema().getFields()) {
            String column = field.name();
            if (!column.equals(rowKeyField) && (record.get(column) != null)) {
                byte[] valueBytes =
                        convertFieldToBytes(record.get(column), field.schema().getType());
                entry.setCell(
                        columnFamilyName,
                        ByteString.copyFromUtf8(column),
                        BigtableUtils.getTimestamp(context),
                        ByteString.copyFrom(valueBytes));
            }
        }
        return entry;
    }

    /**
     * Converts a field value from a {@link GenericRecord} to a byte array based on its {@link
     * Type}.
     *
     * @param obj The field value to convert.
     * @param type The {@link Type} of the field.
     * @return The byte array representation of the field value.
     * @throws IllegalArgumentException If the Avro type is not supported.
     */
    @VisibleForTesting
    static byte[] convertFieldToBytes(Object obj, Type type) {
        switch (type) {
            case STRING:
                return ((String) obj).getBytes();
            case BYTES:
                return ((ByteBuffer) obj).array();
            case INT:
                return ByteBuffer.allocate(Integer.BYTES).putInt((Integer) obj).array();
            case LONG:
                return ByteBuffer.allocate(Long.BYTES).putLong((Long) obj).array();
            case FLOAT:
                return ByteBuffer.allocate(Float.BYTES).putFloat((Float) obj).array();
            case DOUBLE:
                return ByteBuffer.allocate(Double.BYTES).putDouble((Double) obj).array();
            case BOOLEAN:
                return ByteBuffer.allocate(Byte.BYTES).put((byte) ((Boolean) obj ? 1 : 0)).array();
            case RECORD:
                throw new IllegalArgumentException(ErrorMessages.NESTED_TYPE_ERROR);
            default:
                throw new IllegalArgumentException(
                        ErrorMessages.UNSUPPORTED_SERIALIZATION_TYPE + type);
        }
    }

    /**
     * Creates a new {@link Builder} for constructing {@code GenericRecordToRowMutationSerializer}
     * instances.
     *
     * @return A new {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** A builder class for creating {@code GenericRecordToRowMutationSerializer} instances. */
    public static class Builder {
        private String rowKeyField;
        private String columnFamily;
        private Boolean useNestedRowsMode = false;

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

        public GenericRecordToRowMutationSerializer build() {
            checkNotNull(this.rowKeyField, ErrorMessages.ROW_KEY_FIELD_NULL);
            if (columnFamily != null && useNestedRowsMode) {
                throw new IllegalArgumentException(
                        ErrorMessages.COLUMN_FAMILY_AND_NESTED_INCOMPATIBLE);
            } else if (columnFamily == null && !useNestedRowsMode) {
                throw new IllegalArgumentException(
                        ErrorMessages.COLUMN_FAMILY_OR_NESTED_ROWS_REQUIRED);
            }
            return new GenericRecordToRowMutationSerializer(
                    rowKeyField, useNestedRowsMode, columnFamily);
        }
    }
}
