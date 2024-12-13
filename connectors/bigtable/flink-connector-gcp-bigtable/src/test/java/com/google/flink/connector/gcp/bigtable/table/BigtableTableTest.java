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

package com.google.flink.connector.gcp.bigtable.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.utils.FactoryMocks;

import com.google.flink.connector.gcp.bigtable.table.config.BigtableConnectorOptions;
import com.google.flink.connector.gcp.bigtable.testingutils.TestingUtils;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the {@link BigtableDynamicTableSink} class.
 *
 * <p>This class verifies the functionality of the {@link BigtableDynamicTableSink} by testing its
 * ability to create a Bigtable sink with different configurations, including column family mode and
 * nested rows mode. It also includes tests for compatibility checks and row key validation.
 */
public class BigtableTableTest {
    private static final List<Column> SCHEMA_LIST =
            Arrays.asList(
                    Column.physical(TestingUtils.ROW_KEY_FIELD, DataTypes.STRING().notNull()),
                    Column.physical(TestingUtils.STRING_FIELD, DataTypes.STRING()),
                    Column.physical(TestingUtils.INTEGER_FIELD, DataTypes.INT()));

    private static final List<Column> NESTED_SCHEMA_LIST =
            Arrays.asList(
                    Column.physical(TestingUtils.ROW_KEY_FIELD, DataTypes.STRING().notNull()),
                    Column.physical(
                            TestingUtils.NESTED_COLUMN_FAMILY_1,
                            DataTypes.ROW(
                                    DataTypes.FIELD(TestingUtils.STRING_FIELD, DataTypes.STRING()),
                                    DataTypes.FIELD(TestingUtils.INTEGER_FIELD, DataTypes.INT()))),
                    Column.physical(
                            TestingUtils.NESTED_COLUMN_FAMILY_2,
                            DataTypes.ROW(
                                    DataTypes.FIELD(
                                            TestingUtils.STRING_FIELD_2, DataTypes.STRING()))));

    @Test
    public void testDynamicTableBigtableSink() throws IOException {
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.COLUMN_FAMILY.key(), TestingUtils.COLUMN_FAMILY);

        ResolvedSchema schema =
                new ResolvedSchema(
                        SCHEMA_LIST,
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey(
                                "primaryKey", Arrays.asList(TestingUtils.ROW_KEY_FIELD)));

        BigtableDynamicTableSink sink =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);

        ReadableConfig connectorOptions = sink.connectorOptions;
        assertEquals(
                connectorOptions.get(BigtableConnectorOptions.COLUMN_FAMILY),
                TestingUtils.COLUMN_FAMILY);
        assertFalse(connectorOptions.get(BigtableConnectorOptions.USE_NESTED_ROWS_MODE));
        assertEquals(connectorOptions.get(BigtableConnectorOptions.TABLE), TestingUtils.TABLE);
        assertEquals(
                connectorOptions.get(BigtableConnectorOptions.INSTANCE), TestingUtils.INSTANCE);
        assertEquals(connectorOptions.get(BigtableConnectorOptions.PROJECT), TestingUtils.PROJECT);

        String rowKeyField =
                sink.resolvedSchema
                        .getColumn(sink.resolvedSchema.getPrimaryKeyIndexes()[0])
                        .get()
                        .getName();
        assertEquals(rowKeyField, TestingUtils.ROW_KEY_FIELD);
        assertEquals(sink.resolvedSchema, schema);
    }

    @Test
    public void testDynamicTableBigtableSinkNested() throws IOException {
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.USE_NESTED_ROWS_MODE.key(), "true");

        ResolvedSchema schema =
                new ResolvedSchema(
                        NESTED_SCHEMA_LIST,
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey(
                                "primaryKey", Arrays.asList(TestingUtils.ROW_KEY_FIELD)));

        BigtableDynamicTableSink sink =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);

        ReadableConfig connectorOptions = sink.connectorOptions;
        assertNull(connectorOptions.get(BigtableConnectorOptions.COLUMN_FAMILY));
        assertTrue(connectorOptions.get(BigtableConnectorOptions.USE_NESTED_ROWS_MODE));
        assertEquals(connectorOptions.get(BigtableConnectorOptions.TABLE), TestingUtils.TABLE);
        assertEquals(
                connectorOptions.get(BigtableConnectorOptions.INSTANCE), TestingUtils.INSTANCE);
        assertEquals(connectorOptions.get(BigtableConnectorOptions.PROJECT), TestingUtils.PROJECT);

        String rowKeyField =
                sink.resolvedSchema
                        .getColumn(sink.resolvedSchema.getPrimaryKeyIndexes()[0])
                        .get()
                        .getName();
        assertEquals(rowKeyField, TestingUtils.ROW_KEY_FIELD);
        assertEquals(sink.resolvedSchema, schema);
    }

    @ParameterizedTest
    @MethodSource("rowKeyCases")
    public void testRowKeyValidation(UniqueConstraint rowKey, String expectedError) {
        ResolvedSchema schema = new ResolvedSchema(SCHEMA_LIST, Collections.emptyList(), rowKey);
        Assertions.assertThatThrownBy(() -> new BigtableDynamicTableSink(schema, null))
                .hasMessageContaining(expectedError);
    }

    private static Stream<Arguments> rowKeyCases() {
        return Stream.of(
                Arguments.of(null, "There must be exactly one primary key"),
                Arguments.of(
                        UniqueConstraint.primaryKey(
                                "many-keys",
                                Arrays.asList(
                                        TestingUtils.ROW_KEY_FIELD, TestingUtils.STRING_FIELD)),
                        "There must be exactly one primary key"),
                Arguments.of(
                        UniqueConstraint.primaryKey(
                                "integer-key", Arrays.asList(TestingUtils.INTEGER_FIELD)),
                        "Row Key needs to be type STRING"));
    }

    private static Map<String, String> getRequiredOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(FactoryUtil.CONNECTOR.key(), "bigtable");
        options.put(BigtableConnectorOptions.INSTANCE.key(), TestingUtils.INSTANCE);
        options.put(BigtableConnectorOptions.TABLE.key(), TestingUtils.TABLE);
        options.put(BigtableConnectorOptions.PROJECT.key(), TestingUtils.PROJECT);
        return options;
    }
}
