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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Factory for the {@code bigtable-changestream} source connector.
 *
 * <p>Registered via SPI in {@code META-INF/services/org.apache.flink.table.factories.Factory}.
 *
 * <p>Supports pluggable formats via the standard {@code format} option (e.g. protobuf, json, avro).
 */
public class BigtableChangeStreamDynamicTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "bigtable-changestream";

    static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project").stringType().noDefaultValue();

    static final ConfigOption<String> INSTANCE =
            ConfigOptions.key("instance").stringType().noDefaultValue();

    static final ConfigOption<String> TABLE =
            ConfigOptions.key("table").stringType().noDefaultValue();

    static final ConfigOption<String> COLUMN_FAMILY =
            ConfigOptions.key("column-family").stringType().noDefaultValue();

    static final ConfigOption<String> APP_PROFILE =
            ConfigOptions.key("app-profile-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Bigtable app profile ID. Change streams require an app profile "
                                    + "configured for single-cluster routing.");

    static final ConfigOption<String> CELL_COLUMN =
            ConfigOptions.key("cell-column").stringType().defaultValue("payload");

    static final ConfigOption<String> ROW_KEY_FIELD =
            ConfigOptions.key("row-key-field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the Flink schema field that maps to the Bigtable row key. "
                                    + "When set, the row key from each ChangeStreamMutation is parsed and "
                                    + "injected into this field, since the write path typically excludes the "
                                    + "primary key from the proto payload.");

    static final ConfigOption<Integer> START_LOOKBACK_SECONDS =
            ConfigOptions.key("start-lookback-seconds").intType().defaultValue(300);

    static final ConfigOption<Integer> BUFFER_CAPACITY =
            ConfigOptions.key("buffer-capacity")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Bounded buffer capacity for records produced by stream threads. "
                                    + "Stream threads block when the buffer is full, providing backpressure.");

    static final ConfigOption<Integer> GRPC_CHANNEL_POOL_SIZE =
            ConfigOptions.key("grpc-channel-pool-size")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Number of gRPC channels in the pool. 0 uses the client default.");

    static final ConfigOption<Integer> MAX_PARTITION_THREADS =
            ConfigOptions.key("max-partition-threads")
                    .intType()
                    .defaultValue(64)
                    .withDescription(
                            "Maximum number of threads for concurrent partition reading. "
                                    + "Each thread reads one Bigtable partition.");

    static final ConfigOption<String> CHANGELOG_MODE =
            ConfigOptions.key("changelog-mode")
                    .stringType()
                    .defaultValue("insert-only")
                    .withDescription(
                            "Controls which Bigtable mutation types are emitted. "
                                    + "'insert-only' emits only SetCell entries as INSERT rows. "
                                    + "'all' also emits DeleteCells/DeleteFamily entries as DELETE "
                                    + "rows (requires row-key-field to be set).");

    static final ConfigOption<Boolean> FAIL_ON_DESERIALIZATION_ERROR =
            ConfigOptions.key("fail-on-deserialization-error")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When true, the job fails on deserialization errors instead of "
                                    + "skipping the malformed record. Default is false (skip and log).");

    static final ConfigOption<Integer> PARALLELISM =
            ConfigOptions.key("parallelism")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Source parallelism override. 0 uses the environment default.");

    private static final Set<ConfigOption<?>> REQUIRED_OPTIONS;
    private static final Set<ConfigOption<?>> OPTIONAL_OPTIONS;

    static {
        Set<ConfigOption<?>> req = new HashSet<>();
        req.add(PROJECT);
        req.add(INSTANCE);
        req.add(TABLE);
        req.add(COLUMN_FAMILY);
        req.add(FactoryUtil.FORMAT);
        REQUIRED_OPTIONS = Collections.unmodifiableSet(req);

        Set<ConfigOption<?>> opt = new HashSet<>();
        opt.add(APP_PROFILE);
        opt.add(CELL_COLUMN);
        opt.add(ROW_KEY_FIELD);
        opt.add(START_LOOKBACK_SECONDS);
        opt.add(BUFFER_CAPACITY);
        opt.add(GRPC_CHANNEL_POOL_SIZE);
        opt.add(MAX_PARTITION_THREADS);
        opt.add(CHANGELOG_MODE);
        opt.add(FAIL_ON_DESERIALIZATION_ERROR);
        opt.add(PARALLELISM);
        OPTIONAL_OPTIONS = Collections.unmodifiableSet(opt);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return REQUIRED_OPTIONS;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return OPTIONAL_OPTIONS;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        helper.validate();

        String changelogMode = helper.getOptions().get(CHANGELOG_MODE);
        String rowKeyField = helper.getOptions().get(ROW_KEY_FIELD);
        if ("all".equals(changelogMode) && (rowKeyField == null || rowKeyField.isEmpty())) {
            throw new ValidationException(
                    "changelog-mode='all' requires 'row-key-field' to be set — "
                            + "DELETE rows need a key to identify the deleted row.");
        }

        DataType physicalSchema =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        RowType rowType = (RowType) physicalSchema.getLogicalType();

        return new BigtableChangeStreamDynamicTableSource(
                helper.getOptions().get(PROJECT),
                helper.getOptions().get(INSTANCE),
                helper.getOptions().get(TABLE),
                helper.getOptions().get(APP_PROFILE),
                helper.getOptions().get(COLUMN_FAMILY),
                helper.getOptions().get(CELL_COLUMN),
                decodingFormat,
                rowType,
                rowKeyField,
                helper.getOptions().get(START_LOOKBACK_SECONDS),
                helper.getOptions().get(BUFFER_CAPACITY),
                helper.getOptions().get(GRPC_CHANNEL_POOL_SIZE),
                helper.getOptions().get(MAX_PARTITION_THREADS),
                changelogMode,
                helper.getOptions().get(FAIL_ON_DESERIALIZATION_ERROR),
                helper.getOptions().get(PARALLELISM));
    }
}
