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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.variant.Variant;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Wraps a {@link DeserializationSchema} and optionally injects the Bigtable row key into a
 * designated field position.
 *
 * <p>This allows the connector to remain format-agnostic: any Flink format (protobuf, JSON, Avro,
 * etc.) deserializes the cell value bytes, and this wrapper handles the Bigtable-specific row key
 * injection afterward.
 */
public class RowKeyInjectingDeserializationSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Sentinel value indicating that no row-key field is configured. */
    public static final int NO_ROW_KEY_INDEX = -1;

    private final DeserializationSchema<RowData> inner;
    private final int rowKeyFieldIndex;
    private final LogicalTypeRoot rowKeyTypeRoot;
    private final RowType rowType;

    /**
     * @param inner the format-provided deserialization schema
     * @param rowKeyFieldIndex index of the row-key field in the schema, or -1 if no injection
     * @param rowKeyTypeRoot logical type of the row-key field (may be null if index is -1)
     * @param rowType the row type of the schema (used for type-safe field access)
     */
    public RowKeyInjectingDeserializationSchema(
            DeserializationSchema<RowData> inner,
            int rowKeyFieldIndex,
            LogicalTypeRoot rowKeyTypeRoot,
            RowType rowType) {
        this.inner = inner;
        this.rowKeyFieldIndex = rowKeyFieldIndex;
        this.rowKeyTypeRoot = rowKeyTypeRoot;
        this.rowType = rowType;
    }

    /** Returns the row type of the schema. */
    public RowType getRowType() {
        return rowType;
    }

    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        inner.open(context);
    }

    public RowData deserialize(byte[] bytes) throws IOException {
        return inner.deserialize(bytes);
    }

    /**
     * Deserializes cell value bytes and injects the Bigtable row key into the designated field.
     *
     * <p>If no row-key field is configured (index == -1), this is equivalent to {@link
     * #deserialize(byte[])}.
     *
     * @param bytes the cell value bytes to deserialize
     * @param rowKeyBytes the raw Bigtable row key bytes (preserves binary keys without corruption)
     */
    public RowData deserializeWithRowKey(byte[] bytes, byte[] rowKeyBytes) throws IOException {
        RowData base = inner.deserialize(bytes);
        if (rowKeyFieldIndex == NO_ROW_KEY_INDEX || rowKeyBytes == null || base == null) {
            return base;
        }

        Object parsedKey = parseRowKey(rowKeyBytes, rowKeyTypeRoot);
        return new RowKeyInjectingRowData(base, rowKeyFieldIndex, parsedKey);
    }

    /** Returns whether a row-key field is configured, enabling delete row emission. */
    public boolean hasRowKeyField() {
        return rowKeyFieldIndex != NO_ROW_KEY_INDEX;
    }

    /**
     * Creates a {@link RowKind#DELETE} {@link RowData} with only the row key field populated.
     *
     * <p>All fields are null except the row-key field, which is set to the parsed row key value.
     * Used for Bigtable delete entries (DeleteCells, DeleteFamily) which carry no cell value
     * payload — only the row key identifies what was deleted.
     *
     * @param rowKeyBytes the raw Bigtable row key bytes
     * @return a DELETE RowData, or {@code null} if no row-key field is configured
     */
    public RowData createDeleteRow(byte[] rowKeyBytes) {
        if (rowKeyFieldIndex == NO_ROW_KEY_INDEX || rowKeyBytes == null) {
            return null;
        }
        GenericRowData row = new GenericRowData(rowType.getFieldCount());
        row.setField(rowKeyFieldIndex, parseRowKey(rowKeyBytes, rowKeyTypeRoot));
        row.setRowKind(RowKind.DELETE);
        return row;
    }

    /**
     * Parses raw Bigtable row key bytes into the appropriate Flink internal type.
     *
     * <p>For {@code VARBINARY}/{@code BINARY}, the raw bytes are returned directly. For string and
     * numeric types, the bytes are decoded as UTF-8 and parsed accordingly.
     *
     * <p><b>Note:</b> Numeric types ({@code BIGINT}, {@code INTEGER}, etc.) assume the row key is a
     * UTF-8 string representation of the number (e.g. {@code "12345"}). If your row keys use
     * binary-encoded numerics (e.g. {@code ByteBuffer.putLong()}), map the row-key field to {@code
     * VARBINARY} instead and decode in downstream logic.
     */
    static Object parseRowKey(byte[] rowKeyBytes, LogicalTypeRoot typeRoot) {
        switch (typeRoot) {
            case VARBINARY:
            case BINARY:
                return rowKeyBytes;
            case BIGINT:
                return Long.parseLong(new String(rowKeyBytes, StandardCharsets.UTF_8));
            case INTEGER:
                return Integer.parseInt(new String(rowKeyBytes, StandardCharsets.UTF_8));
            case SMALLINT:
                return Short.parseShort(new String(rowKeyBytes, StandardCharsets.UTF_8));
            case TINYINT:
                return Byte.parseByte(new String(rowKeyBytes, StandardCharsets.UTF_8));
            case VARCHAR:
            case CHAR:
                return StringData.fromString(new String(rowKeyBytes, StandardCharsets.UTF_8));
            default:
                throw new UnsupportedOperationException(
                        "Unsupported row key type for deserialization: " + typeRoot);
        }
    }

    /**
     * Resolves the row-key field index and type from the schema.
     *
     * @return metadata about the row-key field, or empty if rowKeyField is not configured
     * @throws IllegalArgumentException if the field name is specified but not found in the schema
     */
    static Optional<RowKeyMetadata> resolveRowKeyField(RowType rowType, String rowKeyField) {
        if (rowKeyField == null || rowKeyField.isEmpty()) {
            return Optional.empty();
        }
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (rowType.getFields().get(i).getName().equals(rowKeyField)) {
                return Optional.of(
                        new RowKeyMetadata(i, rowType.getFields().get(i).getType().getTypeRoot()));
            }
        }
        throw new IllegalArgumentException(
                "row-key-field '" + rowKeyField + "' not found in schema: " + rowType);
    }

    /**
     * Delegating {@link RowData} that overrides a single field with the injected row key value.
     *
     * <p>Avoids copying all fields into a new {@link GenericRowData} on every record — delegates
     * all field access to the base row except the row-key index.
     */
    private static final class RowKeyInjectingRowData implements RowData {

        private final RowData base;
        private final int rowKeyIndex;
        private final Object rowKeyValue;

        RowKeyInjectingRowData(RowData base, int rowKeyIndex, Object rowKeyValue) {
            this.base = base;
            this.rowKeyIndex = rowKeyIndex;
            this.rowKeyValue = rowKeyValue;
        }

        @Override
        public int getArity() {
            return base.getArity();
        }

        @Override
        public RowKind getRowKind() {
            return base.getRowKind();
        }

        @Override
        public void setRowKind(RowKind kind) {
            base.setRowKind(kind);
        }

        @Override
        public boolean isNullAt(int pos) {
            return pos == rowKeyIndex ? rowKeyValue == null : base.isNullAt(pos);
        }

        @Override
        public boolean getBoolean(int pos) {
            return base.getBoolean(pos);
        }

        @Override
        public byte getByte(int pos) {
            return pos == rowKeyIndex ? (Byte) rowKeyValue : base.getByte(pos);
        }

        @Override
        public short getShort(int pos) {
            return pos == rowKeyIndex ? (Short) rowKeyValue : base.getShort(pos);
        }

        @Override
        public int getInt(int pos) {
            return pos == rowKeyIndex ? (Integer) rowKeyValue : base.getInt(pos);
        }

        @Override
        public long getLong(int pos) {
            return pos == rowKeyIndex ? (Long) rowKeyValue : base.getLong(pos);
        }

        @Override
        public float getFloat(int pos) {
            return base.getFloat(pos);
        }

        @Override
        public double getDouble(int pos) {
            return base.getDouble(pos);
        }

        @Override
        public StringData getString(int pos) {
            return pos == rowKeyIndex ? (StringData) rowKeyValue : base.getString(pos);
        }

        @Override
        public DecimalData getDecimal(int pos, int precision, int scale) {
            return base.getDecimal(pos, precision, scale);
        }

        @Override
        public TimestampData getTimestamp(int pos, int precision) {
            return base.getTimestamp(pos, precision);
        }

        @Override
        public <T> RawValueData<T> getRawValue(int pos) {
            return base.getRawValue(pos);
        }

        @Override
        public byte[] getBinary(int pos) {
            return pos == rowKeyIndex ? (byte[]) rowKeyValue : base.getBinary(pos);
        }

        @Override
        public ArrayData getArray(int pos) {
            return base.getArray(pos);
        }

        @Override
        public MapData getMap(int pos) {
            return base.getMap(pos);
        }

        @Override
        public RowData getRow(int pos, int numFields) {
            return base.getRow(pos, numFields);
        }

        @Override
        public Variant getVariant(int pos) {
            return base.getVariant(pos);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RowKeyInjectingRowData)) {
                return false;
            }
            RowKeyInjectingRowData that = (RowKeyInjectingRowData) o;
            return rowKeyIndex == that.rowKeyIndex
                    && Objects.equals(base, that.base)
                    && Objects.deepEquals(rowKeyValue, that.rowKeyValue);
        }

        @Override
        public int hashCode() {
            int valueHash =
                    rowKeyValue instanceof byte[]
                            ? Arrays.hashCode((byte[]) rowKeyValue)
                            : Objects.hashCode(rowKeyValue);
            return 31 * (31 * Objects.hashCode(base) + rowKeyIndex) + valueHash;
        }
    }

    /** Encapsulates the resolved index and logical type of a row-key field. */
    static final class RowKeyMetadata {
        private final int fieldIndex;
        private final LogicalTypeRoot typeRoot;

        RowKeyMetadata(int fieldIndex, LogicalTypeRoot typeRoot) {
            this.fieldIndex = fieldIndex;
            this.typeRoot = typeRoot;
        }

        public int getFieldIndex() {
            return fieldIndex;
        }

        public LogicalTypeRoot getTypeRoot() {
            return typeRoot;
        }
    }
}
