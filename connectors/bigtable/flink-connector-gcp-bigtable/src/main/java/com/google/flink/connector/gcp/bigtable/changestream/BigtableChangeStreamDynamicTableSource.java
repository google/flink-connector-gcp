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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;

/**
 * Flink SQL {@link ScanTableSource} that reads from Bigtable Change Streams.
 *
 * <p>Uses a FLIP-27 {@link BigtableChangeStreamSource} for partition-aware reading with per-split
 * continuation tokens. Emits INSERT-only rows decoded via a pluggable {@link DecodingFormat}.
 */
public class BigtableChangeStreamDynamicTableSource implements ScanTableSource {

    private final String projectId;
    private final String instanceId;
    private final String tableId;
    private final String columnFamily;
    private final String cellColumn;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final RowType rowType;
    private final String rowKeyField;
    private final int startLookbackSeconds;
    private final int bufferCapacity;
    private final int grpcChannelPoolSize;
    private final int parallelism;

    public BigtableChangeStreamDynamicTableSource(
            String projectId,
            String instanceId,
            String tableId,
            String columnFamily,
            String cellColumn,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            RowType rowType,
            String rowKeyField,
            int startLookbackSeconds,
            int bufferCapacity,
            int grpcChannelPoolSize,
            int parallelism) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.tableId = tableId;
        this.columnFamily = columnFamily;
        this.cellColumn = cellColumn;
        this.decodingFormat = decodingFormat;
        this.rowType = rowType;
        this.rowKeyField = rowKeyField;
        this.startLookbackSeconds = startLookbackSeconds;
        this.bufferCapacity = bufferCapacity;
        this.grpcChannelPoolSize = grpcChannelPoolSize;
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment env) {

                // Create the format-provided deserialization schema
                DataType physicalDataType = TypeConversions.fromLogicalToDataType(rowType);
                DeserializationSchema<RowData> innerSchema =
                        decodingFormat.createRuntimeDecoder(scanContext, physicalDataType);

                // Resolve row-key field index and type
                int rowKeyFieldIndex = -1;
                LogicalTypeRoot rowKeyTypeRoot = null;
                Object[] resolved =
                        RowKeyInjectingDeserializationSchema.resolveRowKeyField(
                                rowType, rowKeyField);
                if (resolved != null) {
                    rowKeyFieldIndex = (int) resolved[0];
                    rowKeyTypeRoot = (LogicalTypeRoot) resolved[1];
                }

                RowKeyInjectingDeserializationSchema schema =
                        new RowKeyInjectingDeserializationSchema(
                                innerSchema, rowKeyFieldIndex, rowKeyTypeRoot, rowType);

                BigtableChangeStreamSource source =
                        new BigtableChangeStreamSource(
                                projectId,
                                instanceId,
                                tableId,
                                columnFamily,
                                cellColumn,
                                schema,
                                startLookbackSeconds,
                                bufferCapacity,
                                grpcChannelPoolSize);

                int p = parallelism > 0 ? parallelism : env.getParallelism();

                return env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "bigtable-changestream-source")
                        .setParallelism(p)
                        .returns(InternalTypeInfo.of(rowType));
            }

            @Override
            public boolean isBounded() {
                return false;
            }
        };
    }

    @Override
    public DynamicTableSource copy() {
        return new BigtableChangeStreamDynamicTableSource(
                projectId,
                instanceId,
                tableId,
                columnFamily,
                cellColumn,
                decodingFormat,
                rowType,
                rowKeyField,
                startLookbackSeconds,
                bufferCapacity,
                grpcChannelPoolSize,
                parallelism);
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "BigtableChangeStreamSource(project=%s, instance=%s, table=%s, family=%s, "
                        + "column=%s)",
                projectId, instanceId, tableId, columnFamily, cellColumn);
    }
}
