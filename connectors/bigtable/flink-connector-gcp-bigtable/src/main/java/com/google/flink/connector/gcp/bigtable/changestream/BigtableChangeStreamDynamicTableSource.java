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

import java.util.Optional;

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
    private final String appProfileId;
    private final String columnFamily;
    private final String cellColumn;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final RowType rowType;
    private final String rowKeyField;
    private final int startLookbackSeconds;
    private final int bufferCapacity;
    private final int grpcChannelPoolSize;
    private final int maxPartitionThreads;
    private final String changelogMode;
    private final int parallelism;

    public BigtableChangeStreamDynamicTableSource(
            String projectId,
            String instanceId,
            String tableId,
            String appProfileId,
            String columnFamily,
            String cellColumn,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            RowType rowType,
            String rowKeyField,
            int startLookbackSeconds,
            int bufferCapacity,
            int grpcChannelPoolSize,
            int maxPartitionThreads,
            String changelogMode,
            int parallelism) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.tableId = tableId;
        this.appProfileId = appProfileId;
        this.columnFamily = columnFamily;
        this.cellColumn = cellColumn;
        this.decodingFormat = decodingFormat;
        this.rowType = rowType;
        this.rowKeyField = rowKeyField;
        this.startLookbackSeconds = startLookbackSeconds;
        this.bufferCapacity = bufferCapacity;
        this.grpcChannelPoolSize = grpcChannelPoolSize;
        this.maxPartitionThreads = maxPartitionThreads;
        this.changelogMode = changelogMode;
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // Bigtable Change Stream entry types map to Flink RowKind as follows:
        //
        //   SetCell      → RowKind.INSERT  — a cell value was written; deserialize and emit.
        //   DeleteCells  → RowKind.DELETE  — specific cells were deleted for this row.
        //   DeleteFamily → RowKind.DELETE  — an entire column family was deleted for this row.
        //
        // Each ChangeStreamMutation is scoped to a single row key. A DeleteFamily does NOT
        // fan out — application-level batch deletes produce one mutation per row, each
        // emitting one DELETE RowData. Cardinality is always: one mutation = one emitted row.
        //
        // Mixed mutations (SetCell + delete entries): emit INSERT for the matching SetCell
        // only. The delete entries are secondary (e.g. "delete old version, set new value").
        // If no matching SetCell exists but deletes are present, emit DELETE.
        //
        // DELETE emission is controlled by the 'changelog-mode' option:
        //   'insert-only' (default) — only SetCell entries are emitted as INSERT.
        //   'all' — also emits DeleteCells/DeleteFamily as DELETE (requires row-key-field).
        ChangelogMode.Builder builder = ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT);
        if ("all".equals(changelogMode) && rowKeyField != null && !rowKeyField.isEmpty()) {
            builder.addContainedKind(RowKind.DELETE);
        }
        return builder.build();
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
                int rowKeyFieldIndex = RowKeyInjectingDeserializationSchema.NO_ROW_KEY_INDEX;
                LogicalTypeRoot rowKeyTypeRoot = null;
                Optional<RowKeyInjectingDeserializationSchema.RowKeyMetadata> resolved =
                        RowKeyInjectingDeserializationSchema.resolveRowKeyField(
                                rowType, rowKeyField);
                if (resolved.isPresent()) {
                    rowKeyFieldIndex = resolved.get().getFieldIndex();
                    rowKeyTypeRoot = resolved.get().getTypeRoot();
                }

                RowKeyInjectingDeserializationSchema schema =
                        new RowKeyInjectingDeserializationSchema(
                                innerSchema, rowKeyFieldIndex, rowKeyTypeRoot, rowType);

                boolean emitDeletes =
                        "all".equals(changelogMode)
                                && rowKeyField != null
                                && !rowKeyField.isEmpty();

                BigtableChangeStreamSource source =
                        new BigtableChangeStreamSource(
                                projectId,
                                instanceId,
                                tableId,
                                appProfileId,
                                columnFamily,
                                cellColumn,
                                schema,
                                emitDeletes,
                                startLookbackSeconds,
                                bufferCapacity,
                                grpcChannelPoolSize,
                                maxPartitionThreads);

                int p = parallelism > 0 ? parallelism : env.getParallelism();

                return env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                BigtableChangeStreamDynamicTableFactory.IDENTIFIER)
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
                appProfileId,
                columnFamily,
                cellColumn,
                decodingFormat,
                rowType,
                rowKeyField,
                startLookbackSeconds,
                bufferCapacity,
                grpcChannelPoolSize,
                maxPartitionThreads,
                changelogMode,
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
