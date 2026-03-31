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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RowKeyInjectingDeserializationSchemaTest {

    @Test
    void parseRowKeyBigint() {
        assertEquals(
                123456789L,
                RowKeyInjectingDeserializationSchema.parseRowKey(
                        "123456789".getBytes(StandardCharsets.UTF_8), LogicalTypeRoot.BIGINT));
    }

    @Test
    void parseRowKeyInteger() {
        assertEquals(
                42,
                RowKeyInjectingDeserializationSchema.parseRowKey(
                        "42".getBytes(StandardCharsets.UTF_8), LogicalTypeRoot.INTEGER));
    }

    @Test
    void parseRowKeySmallint() {
        assertEquals(
                (short) 7,
                RowKeyInjectingDeserializationSchema.parseRowKey(
                        "7".getBytes(StandardCharsets.UTF_8), LogicalTypeRoot.SMALLINT));
    }

    @Test
    void parseRowKeyTinyint() {
        assertEquals(
                (byte) 3,
                RowKeyInjectingDeserializationSchema.parseRowKey(
                        "3".getBytes(StandardCharsets.UTF_8), LogicalTypeRoot.TINYINT));
    }

    @Test
    void parseRowKeyVarchar() {
        assertEquals(
                StringData.fromString("my-key"),
                RowKeyInjectingDeserializationSchema.parseRowKey(
                        "my-key".getBytes(StandardCharsets.UTF_8), LogicalTypeRoot.VARCHAR));
    }

    @Test
    void parseRowKeyChar() {
        assertEquals(
                StringData.fromString("abc"),
                RowKeyInjectingDeserializationSchema.parseRowKey(
                        "abc".getBytes(StandardCharsets.UTF_8), LogicalTypeRoot.CHAR));
    }

    @Test
    void parseRowKeyVarbinary() {
        byte[] binaryKey = new byte[] {0x00, 0x01, (byte) 0xFF, 0x7F};
        assertArrayEquals(
                binaryKey,
                (byte[])
                        RowKeyInjectingDeserializationSchema.parseRowKey(
                                binaryKey, LogicalTypeRoot.VARBINARY));
    }

    @Test
    void parseRowKeyBinary() {
        byte[] binaryKey = new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
        assertArrayEquals(
                binaryKey,
                (byte[])
                        RowKeyInjectingDeserializationSchema.parseRowKey(
                                binaryKey, LogicalTypeRoot.BINARY));
    }

    @Test
    void parseRowKeyUnsupportedTypeThrows() {
        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        RowKeyInjectingDeserializationSchema.parseRowKey(
                                "1.5".getBytes(StandardCharsets.UTF_8), LogicalTypeRoot.DOUBLE));
    }

    @Test
    void resolveRowKeyFieldFindsField() {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("id", new BigIntType()),
                                new RowType.RowField("name", new VarCharType()),
                                new RowType.RowField("age", new IntType())));

        Optional<RowKeyInjectingDeserializationSchema.RowKeyMetadata> result =
                RowKeyInjectingDeserializationSchema.resolveRowKeyField(rowType, "id");

        assertTrue(result.isPresent());
        assertEquals(0, result.get().getFieldIndex());
        assertEquals(LogicalTypeRoot.BIGINT, result.get().getTypeRoot());
    }

    @Test
    void resolveRowKeyFieldReturnsEmptyForNull() {
        RowType rowType = new RowType(Arrays.asList(new RowType.RowField("id", new BigIntType())));

        assertFalse(
                RowKeyInjectingDeserializationSchema.resolveRowKeyField(rowType, null).isPresent());
    }

    @Test
    void resolveRowKeyFieldReturnsEmptyForEmptyString() {
        RowType rowType = new RowType(Arrays.asList(new RowType.RowField("id", new BigIntType())));

        assertFalse(
                RowKeyInjectingDeserializationSchema.resolveRowKeyField(rowType, "").isPresent());
    }

    @Test
    void resolveRowKeyFieldThrowsForMissingField() {
        RowType rowType = new RowType(Arrays.asList(new RowType.RowField("id", new BigIntType())));

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        RowKeyInjectingDeserializationSchema.resolveRowKeyField(
                                rowType, "nonexistent"));
    }

    @Test
    void deserializeWithRowKeyInjectsStringKey() throws Exception {
        // Schema: (row_key VARCHAR, payload VARCHAR)
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("row_key", new VarCharType()),
                                new RowType.RowField("payload", new VarCharType())));

        DeserializationSchema<RowData> inner = new FakeDeserializationSchema(rowType);

        RowKeyInjectingDeserializationSchema schema =
                new RowKeyInjectingDeserializationSchema(
                        inner, 0, LogicalTypeRoot.VARCHAR, rowType);

        RowData result =
                schema.deserializeWithRowKey(
                        new byte[] {}, "my-row-key".getBytes(StandardCharsets.UTF_8));
        assertNotNull(result);
        assertEquals(StringData.fromString("my-row-key"), result.getString(0));
        assertEquals(StringData.fromString("hello"), result.getString(1));
    }

    @Test
    void deserializeWithRowKeyInjectsBigintKey() throws Exception {
        // Schema: (id BIGINT, name VARCHAR)
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("id", new BigIntType()),
                                new RowType.RowField("name", new VarCharType())));

        DeserializationSchema<RowData> inner = new FakeDeserializationSchema(rowType);

        RowKeyInjectingDeserializationSchema schema =
                new RowKeyInjectingDeserializationSchema(inner, 0, LogicalTypeRoot.BIGINT, rowType);

        RowData result =
                schema.deserializeWithRowKey(new byte[] {}, "999".getBytes(StandardCharsets.UTF_8));
        assertNotNull(result);
        assertEquals(999L, result.getLong(0));
    }

    @Test
    void deserializeWithRowKeyInjectsBinaryKey() throws Exception {
        // Schema: (row_key VARBINARY, payload VARCHAR)
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("row_key", new VarBinaryType(100)),
                                new RowType.RowField("payload", new VarCharType())));

        DeserializationSchema<RowData> inner = new FakeDeserializationSchema(rowType);

        RowKeyInjectingDeserializationSchema schema =
                new RowKeyInjectingDeserializationSchema(
                        inner, 0, LogicalTypeRoot.VARBINARY, rowType);

        byte[] binaryKey = new byte[] {0x00, 0x01, (byte) 0xFF, 0x7F};
        RowData result = schema.deserializeWithRowKey(new byte[] {}, binaryKey);
        assertNotNull(result);
        assertArrayEquals(binaryKey, result.getBinary(0));
        assertEquals(StringData.fromString("hello"), result.getString(1));
    }

    @Test
    void deserializeWithRowKeyPreservesBinaryKeyWithNonUtf8Bytes() throws Exception {
        // Verifies that binary row keys with non-UTF-8 bytes are not corrupted
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("row_key", new VarBinaryType(100)),
                                new RowType.RowField("payload", new VarCharType())));

        DeserializationSchema<RowData> inner = new FakeDeserializationSchema(rowType);

        RowKeyInjectingDeserializationSchema schema =
                new RowKeyInjectingDeserializationSchema(
                        inner, 0, LogicalTypeRoot.VARBINARY, rowType);

        // Key with bytes that are invalid UTF-8 sequences
        byte[] binaryKey = new byte[] {(byte) 0xC0, (byte) 0xC1, (byte) 0xFE, (byte) 0xFF};
        RowData result = schema.deserializeWithRowKey(new byte[] {}, binaryKey);
        assertNotNull(result);
        assertArrayEquals(binaryKey, result.getBinary(0));
    }

    @Test
    void deserializeWithRowKeySkipsWhenNoRowKeyConfigured() throws Exception {
        RowType rowType =
                new RowType(Arrays.asList(new RowType.RowField("payload", new VarCharType())));

        DeserializationSchema<RowData> inner = new FakeDeserializationSchema(rowType);

        RowKeyInjectingDeserializationSchema schema =
                new RowKeyInjectingDeserializationSchema(
                        inner,
                        RowKeyInjectingDeserializationSchema.NO_ROW_KEY_INDEX,
                        null,
                        rowType);

        RowData result =
                schema.deserializeWithRowKey(new byte[] {}, "key".getBytes(StandardCharsets.UTF_8));
        assertNotNull(result);
        // Should return the inner result unchanged
        assertEquals(StringData.fromString("hello"), result.getString(0));
    }

    @Test
    void deserializeWithRowKeyReturnsNullWhenInnerReturnsNull() throws Exception {
        RowType rowType = new RowType(Arrays.asList(new RowType.RowField("id", new BigIntType())));

        DeserializationSchema<RowData> inner =
                new DeserializationSchema<RowData>() {
                    @Override
                    public RowData deserialize(byte[] message) {
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(RowData nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<RowData> getProducedType() {
                        return null;
                    }
                };

        RowKeyInjectingDeserializationSchema schema =
                new RowKeyInjectingDeserializationSchema(inner, 0, LogicalTypeRoot.BIGINT, rowType);

        assertNull(
                schema.deserializeWithRowKey(
                        new byte[] {}, "123".getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Fake deserialization schema that returns a row with null for field 0 and "hello" for field 1
     * (or field 0 if there's only one field).
     */
    private static class FakeDeserializationSchema implements DeserializationSchema<RowData> {
        private final int fieldCount;

        FakeDeserializationSchema(RowType rowType) {
            this.fieldCount = rowType.getFieldCount();
        }

        @Override
        public RowData deserialize(byte[] message) throws IOException {
            GenericRowData row = new GenericRowData(fieldCount);
            if (fieldCount == 1) {
                row.setField(0, StringData.fromString("hello"));
            } else {
                row.setField(0, null);
                row.setField(1, StringData.fromString("hello"));
            }
            return row;
        }

        @Override
        public boolean isEndOfStream(RowData nextElement) {
            return false;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return null;
        }
    }
}
