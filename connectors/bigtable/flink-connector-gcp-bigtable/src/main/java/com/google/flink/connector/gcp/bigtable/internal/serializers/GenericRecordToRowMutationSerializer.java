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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.flink.connector.gcp.bigtable.internal.utils.BigtableUtils;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;

/**
 * A serializer that converts {@link GenericRecord} objects into Bigtable {@link RowMutationEntry}
 * objects.
 *
 * <p>This serializer supports two modes of operation:
 *
 * <ol>
 *   <li>**Column Family Mode:** All fields in the {@link GenericRecord} are written to a single
 *       specified column family.
 *   <li>**Nested Rows Mode:** Each field in the {@link GenericRecord} (except the row key field)
 *       represents a separate column family, and its value must be another {@link GenericRecord}
 *       containing the columns for that family.
 * </ol>
 *
 * <p>The serialization process involves extracting data from the {@link GenericRecord} fields and
 * converting them to appropriate byte arrays for Bigtable.
 */
public class GenericRecordToRowMutationSerializer
        implements BaseRowMutationSerializer<GenericRecord> {
    private final String rowKeyField;
    private final @Nullable String columnFamily;

    /**
     * Constructs a {@code GenericRecordToRowMutationSerializer}.
     *
     * @param rowKeyField The name of the field in the {@link GenericRecord} that represents the
     *     Bigtable row key.
     * @param columnFamily The name of the column family to use (only in column family mode).
     */
    public GenericRecordToRowMutationSerializer(
            String rowKeyField, Boolean useNestedRows, @Nullable String columnFamily) {
        this.rowKeyField = rowKeyField;
        this.columnFamily = columnFamily;
    }

    @Override
    public RowMutationEntry serialize(GenericRecord record, SinkWriter.Context context) {
        RowMutationEntry entry = RowMutationEntry.create((String) record.get(rowKeyField));

        return serializeWithColumnFamily(record, entry, this.columnFamily, context);
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
            default:
                throw new IllegalArgumentException(
                        "Unsupported Avro type, use Bytes for more complex types: " + type);
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
        private Boolean useNestedFields = false;

        public Builder withRowKeyField(String rowKeyField) {
            this.rowKeyField = rowKeyField;
            return this;
        }

        public Builder withColumnFamily(String columnFamily) {
            this.columnFamily = columnFamily;
            return this;
        }

        public GenericRecordToRowMutationSerializer build() {
            return new GenericRecordToRowMutationSerializer(
                    rowKeyField, useNestedFields, columnFamily);
        }
    }
}
