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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.Serializable;

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

    private final DeserializationSchema<RowData> inner;
    private final int rowKeyFieldIndex;
    private final LogicalTypeRoot rowKeyTypeRoot;
    private final int totalFields;
    private final RowType rowType;

    private transient RowData.FieldGetter[] fieldGetters;

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
        this.totalFields = rowType.getFieldCount();
        this.rowType = rowType;
    }

    /** Returns the row type of the schema. */
    public RowType getRowType() {
        return rowType;
    }

    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        inner.open(context);
        initFieldGetters();
    }

    private void initFieldGetters() {
        if (fieldGetters == null && rowKeyFieldIndex >= 0) {
            fieldGetters = new RowData.FieldGetter[totalFields];
            for (int i = 0; i < totalFields; i++) {
                if (i != rowKeyFieldIndex) {
                    LogicalType fieldType = rowType.getTypeAt(i);
                    fieldGetters[i] = RowData.createFieldGetter(fieldType, i);
                }
            }
        }
    }

    public RowData deserialize(byte[] bytes) throws IOException {
        return inner.deserialize(bytes);
    }

    /**
     * Deserializes cell value bytes and injects the Bigtable row key into the designated field.
     *
     * <p>If no row-key field is configured (index == -1), this is equivalent to {@link
     * #deserialize(byte[])}.
     */
    public RowData deserializeWithRowKey(byte[] bytes, String rowKey) throws IOException {
        RowData base = inner.deserialize(bytes);
        if (rowKeyFieldIndex < 0 || rowKey == null || base == null) {
            return base;
        }

        // Lazy init handles deserialization (transient field not restored)
        initFieldGetters();

        GenericRowData result = new GenericRowData(totalFields);
        for (int i = 0; i < totalFields; i++) {
            if (i == rowKeyFieldIndex) {
                result.setField(i, parseRowKey(rowKey, rowKeyTypeRoot));
            } else {
                result.setField(i, fieldGetters[i].getFieldOrNull(base));
            }
        }
        result.setRowKind(base.getRowKind());
        return result;
    }

    /**
     * Parses a Bigtable row key string into the appropriate Flink internal type.
     *
     * <p>Assumes the row key is a valid UTF-8 string with base-10 numeric encoding for numeric
     * types.
     */
    static Object parseRowKey(String rowKey, LogicalTypeRoot typeRoot) {
        switch (typeRoot) {
            case BIGINT:
                return Long.parseLong(rowKey);
            case INTEGER:
                return Integer.parseInt(rowKey);
            case SMALLINT:
                return Short.parseShort(rowKey);
            case TINYINT:
                return Byte.parseByte(rowKey);
            case VARCHAR:
            case CHAR:
                return StringData.fromString(rowKey);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported row key type for deserialization: " + typeRoot);
        }
    }

    /**
     * Resolves the row-key field index and type from the schema.
     *
     * @return a two-element array: [fieldIndex (int), typeRoot (LogicalTypeRoot)], or null if
     *     rowKeyField is not configured
     */
    static Object[] resolveRowKeyField(RowType rowType, String rowKeyField) {
        if (rowKeyField == null || rowKeyField.isEmpty()) {
            return null;
        }
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (rowType.getFields().get(i).getName().equals(rowKeyField)) {
                return new Object[] {i, rowType.getFields().get(i).getType().getTypeRoot()};
            }
        }
        throw new IllegalArgumentException(
                "row-key-field '" + rowKeyField + "' not found in schema: " + rowType);
    }
}
