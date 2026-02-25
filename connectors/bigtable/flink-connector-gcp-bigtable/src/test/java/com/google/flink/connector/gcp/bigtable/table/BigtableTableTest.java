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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.types.RowKind;

import com.google.flink.connector.gcp.bigtable.table.config.BigtableConnectorOptions;
import com.google.flink.connector.gcp.bigtable.testingutils.TestingUtils;
import com.google.flink.connector.gcp.bigtable.utils.ErrorMessages;
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

        ResolvedSchema schema = getResolvedSchema(false);

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
        assertFalse(connectorOptions.get(BigtableConnectorOptions.FLOW_CONTROL));
        assertNull(connectorOptions.get(BigtableConnectorOptions.APP_PROFILE_ID));

        assertEquals(sink.rowKeyField, TestingUtils.ROW_KEY_FIELD);
        assertEquals(sink.resolvedSchema, schema);
        assertNull(sink.parallelism);
    }

    @Test
    public void testDynamicTableBigtableSinkParallelism() throws IOException {
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.COLUMN_FAMILY.key(), TestingUtils.COLUMN_FAMILY);
        options.put(BigtableConnectorOptions.SINK_PARALLELISM.key(), "2");

        ResolvedSchema schema = getResolvedSchema(false);

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
        assertFalse(connectorOptions.get(BigtableConnectorOptions.FLOW_CONTROL));
        assertNull(connectorOptions.get(BigtableConnectorOptions.APP_PROFILE_ID));

        assertEquals(sink.rowKeyField, TestingUtils.ROW_KEY_FIELD);
        assertEquals((Integer) sink.parallelism, (Integer) 2);
        assertEquals(sink.resolvedSchema, schema);
    }

    @Test
    public void testDynamicTableBigtableSinkNested() throws IOException {
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.USE_NESTED_ROWS_MODE.key(), "true");

        ResolvedSchema schema = getResolvedSchema(true);

        BigtableDynamicTableSink sink =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);

        ReadableConfig connectorOptions = sink.connectorOptions;
        assertNull(connectorOptions.get(BigtableConnectorOptions.COLUMN_FAMILY));
        assertTrue(connectorOptions.get(BigtableConnectorOptions.USE_NESTED_ROWS_MODE));
        assertEquals(connectorOptions.get(BigtableConnectorOptions.TABLE), TestingUtils.TABLE);
        assertEquals(
                connectorOptions.get(BigtableConnectorOptions.INSTANCE), TestingUtils.INSTANCE);
        assertEquals(connectorOptions.get(BigtableConnectorOptions.PROJECT), TestingUtils.PROJECT);
        assertFalse(connectorOptions.get(BigtableConnectorOptions.FLOW_CONTROL));
        assertNull(connectorOptions.get(BigtableConnectorOptions.APP_PROFILE_ID));

        assertEquals(sink.rowKeyField, TestingUtils.ROW_KEY_FIELD);
        assertEquals(sink.resolvedSchema, schema);
        assertNull(sink.parallelism);
    }

    @Test
    public void testDynamicTableBigtableSinkExtraOptions() throws IOException {
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.FLOW_CONTROL.key(), "true");
        options.put(BigtableConnectorOptions.APP_PROFILE_ID.key(), TestingUtils.APP_PROFILE);
        options.put(BigtableConnectorOptions.CREDENTIALS_ACCESS_TOKEN.key(), "token");
        options.put(BigtableConnectorOptions.CREDENTIALS_KEY.key(), "key");
        options.put(BigtableConnectorOptions.CREDENTIALS_FILE.key(), "file");

        ResolvedSchema schema = getResolvedSchema(false);

        BigtableDynamicTableSink sink =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);

        ReadableConfig connectorOptions = sink.connectorOptions;
        assertTrue(connectorOptions.get(BigtableConnectorOptions.FLOW_CONTROL));
        assertEquals(
                connectorOptions.get(BigtableConnectorOptions.APP_PROFILE_ID),
                TestingUtils.APP_PROFILE);
        assertEquals(
                connectorOptions.get(BigtableConnectorOptions.CREDENTIALS_ACCESS_TOKEN), "token");
        assertEquals(connectorOptions.get(BigtableConnectorOptions.CREDENTIALS_KEY), "key");
        assertEquals(connectorOptions.get(BigtableConnectorOptions.CREDENTIALS_FILE), "file");
    }

    @Test
    public void testRequiredConnectorOptions() throws IOException {
        Map<String, String> options = getRequiredOptions();
        ResolvedSchema schema = getResolvedSchema(false);

        DynamicTableFactory.Context context = FactoryMocks.createTableContext(schema, options);
        BigtableDynamicTableFactory factory = new BigtableDynamicTableFactory();

        FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(factory, context);

        helper.validate();
    }

    @Test
    public void testIncorrectConnectorOptions() throws IOException {
        Map<String, String> options = new HashMap<>();
        ResolvedSchema schema = getResolvedSchema(false);

        DynamicTableFactory.Context context = FactoryMocks.createTableContext(schema, options);
        BigtableDynamicTableFactory factory = new BigtableDynamicTableFactory();

        FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(factory, context);

        Assertions.assertThatThrownBy(() -> helper.validate())
                .hasMessageContaining("Missing required options are")
                .hasMessageContaining(BigtableConnectorOptions.PROJECT.key())
                .hasMessageContaining(BigtableConnectorOptions.INSTANCE.key())
                .hasMessageContaining(BigtableConnectorOptions.TABLE.key());
    }

    @Test
    public void testCopy() {
        ResolvedSchema schema = getResolvedSchema(true);

        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.USE_NESTED_ROWS_MODE.key(), "true");

        BigtableDynamicTableSink sink =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);
        BigtableDynamicTableSink sink2 =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);

        assertEquals(sink2, sink.copy());
        assertTrue(sink2.equals(sink.copy()));
    }

    @Test
    public void testCopyParallelism() {
        ResolvedSchema schema = getResolvedSchema(false);

        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.COLUMN_FAMILY.key(), TestingUtils.COLUMN_FAMILY);
        options.put(BigtableConnectorOptions.SINK_PARALLELISM.key(), "2");

        BigtableDynamicTableSink sink =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);
        BigtableDynamicTableSink sink2 =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);

        assertEquals(sink2, sink.copy());
        assertTrue(sink2.equals(sink.copy()));
    }

    @Test
    public void testSummaryString() {
        ResolvedSchema schema = getResolvedSchema(false);

        String parallelism = "2";
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.USE_NESTED_ROWS_MODE.key(), "true");
        options.put(BigtableConnectorOptions.SINK_PARALLELISM.key(), parallelism);

        BigtableDynamicTableSink sink =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);

        String summaryString =
                String.format(
                        "BigtableSink("
                                + "parallelism=%s, "
                                + "connectorOptions=%s, "
                                + "resolvedSchema=%s, "
                                + "rowKeyField=%s)",
                        parallelism, options, schema, TestingUtils.ROW_KEY_FIELD);

        assertEquals(sink.asSummaryString(), summaryString);
    }

    @Test
    public void testSummaryStringNull() {
        ResolvedSchema schema = getResolvedSchema(false);

        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.USE_NESTED_ROWS_MODE.key(), "true");

        BigtableDynamicTableSink sink =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);

        String summaryString =
                String.format(
                        "BigtableSink("
                                + "parallelism=%s, "
                                + "connectorOptions=%s, "
                                + "resolvedSchema=%s, "
                                + "rowKeyField=%s)",
                        null, options, schema, TestingUtils.ROW_KEY_FIELD);

        assertEquals(sink.asSummaryString(), summaryString);
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
                Arguments.of(null, String.format(ErrorMessages.MULTIPLE_PRIMARY_KEYS_TEMPLATE, 0)),
                Arguments.of(
                        UniqueConstraint.primaryKey(
                                "many-keys",
                                Arrays.asList(
                                        TestingUtils.ROW_KEY_FIELD, TestingUtils.STRING_FIELD)),
                        String.format(ErrorMessages.MULTIPLE_PRIMARY_KEYS_TEMPLATE, 2)),
                Arguments.of(
                        UniqueConstraint.primaryKey(
                                "integer-key", Arrays.asList(TestingUtils.INTEGER_FIELD)),
                        String.format(
                                ErrorMessages.ROW_KEY_STRING_TYPE_TEMPLATE, DataTypes.INT())));
    }

    @Test
    public void testUpsertModeChangelogMode() throws IOException {
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.COLUMN_FAMILY.key(), TestingUtils.COLUMN_FAMILY);
        options.put(BigtableConnectorOptions.CHANGELOG_MODE.key(), "upsert");

        ResolvedSchema schema = getResolvedSchema(false);

        BigtableDynamicTableSink sink =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);

        ChangelogMode changelogMode = sink.getChangelogMode(ChangelogMode.insertOnly());
        assertTrue(changelogMode.contains(RowKind.INSERT));
        assertTrue(changelogMode.contains(RowKind.UPDATE_AFTER));
        assertTrue(changelogMode.contains(RowKind.DELETE));
        assertFalse(changelogMode.contains(RowKind.UPDATE_BEFORE));
    }

    @Test
    public void testAllModeChangelogMode() throws IOException {
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.COLUMN_FAMILY.key(), TestingUtils.COLUMN_FAMILY);
        options.put(BigtableConnectorOptions.CHANGELOG_MODE.key(), "all");

        ResolvedSchema schema = getResolvedSchema(false);

        BigtableDynamicTableSink sink =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);

        ChangelogMode changelogMode = sink.getChangelogMode(ChangelogMode.insertOnly());
        assertTrue(changelogMode.contains(RowKind.INSERT));
        assertTrue(changelogMode.contains(RowKind.UPDATE_AFTER));
        assertTrue(changelogMode.contains(RowKind.DELETE));
        assertTrue(changelogMode.contains(RowKind.UPDATE_BEFORE));
    }

    @Test
    public void testNonUpsertModeChangelogMode() throws IOException {
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.COLUMN_FAMILY.key(), TestingUtils.COLUMN_FAMILY);

        ResolvedSchema schema = getResolvedSchema(false);

        BigtableDynamicTableSink sink =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);

        ChangelogMode changelogMode = sink.getChangelogMode(ChangelogMode.insertOnly());
        assertTrue(changelogMode.contains(RowKind.INSERT));
        assertFalse(changelogMode.contains(RowKind.UPDATE_AFTER));
        assertFalse(changelogMode.contains(RowKind.DELETE));
    }

    @Test
    public void testChangelogModeWithoutPrimaryKeyThrows() throws IOException {
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.COLUMN_FAMILY.key(), TestingUtils.COLUMN_FAMILY);
        options.put(BigtableConnectorOptions.CHANGELOG_MODE.key(), "upsert");

        ResolvedSchema schemaWithoutPK =
                new ResolvedSchema(SCHEMA_LIST, Collections.emptyList(), null);

        Assertions.assertThatThrownBy(() -> FactoryMocks.createTableSink(schemaWithoutPK, options))
                .hasStackTraceContaining("requires a PRIMARY KEY");
    }

    @Test
    public void testInvalidChangelogModeThrows() throws IOException {
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.COLUMN_FAMILY.key(), TestingUtils.COLUMN_FAMILY);
        options.put(BigtableConnectorOptions.CHANGELOG_MODE.key(), "bogus");

        ResolvedSchema schema = getResolvedSchema(false);

        Assertions.assertThatThrownBy(() -> FactoryMocks.createTableSink(schema, options))
                .hasStackTraceContaining("Invalid 'changelog-mode' value 'bogus'");
    }

    @Test
    public void testChangelogModeCopy() {
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.COLUMN_FAMILY.key(), TestingUtils.COLUMN_FAMILY);
        options.put(BigtableConnectorOptions.CHANGELOG_MODE.key(), "upsert");

        ResolvedSchema schema = getResolvedSchema(false);

        BigtableDynamicTableSink sink =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);
        BigtableDynamicTableSink sink2 =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);

        assertEquals(sink2, sink.copy());
        assertTrue(sink2.equals(sink.copy()));
    }

    private static ResolvedSchema getResolvedSchema(Boolean useNestedRowsMode) {
        List<Column> schemaList = useNestedRowsMode ? NESTED_SCHEMA_LIST : SCHEMA_LIST;

        return new ResolvedSchema(
                schemaList,
                Collections.emptyList(),
                UniqueConstraint.primaryKey(
                        "primaryKey", Arrays.asList(TestingUtils.ROW_KEY_FIELD)));
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
