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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * Flink SQL {@link ScanTableSource} that reads from Bigtable Change Streams.
 *
 * <p>Uses a FLIP-27 {@link BigtableChangeStreamSource} for partition-aware reading with per-split
 * continuation tokens. Emits INSERT-only rows decoded from proto-encoded Bigtable cells.
 */
public class BigtableChangeStreamDynamicTableSource implements ScanTableSource {

    private final String projectId;
    private final String instanceId;
    private final String tableId;
    private final String columnFamily;
    private final String cellColumn;
    private final String messageClassName;
    private final RowType rowType;
    private final String rowKeyField;
    private final int startLookbackSeconds;

    public BigtableChangeStreamDynamicTableSource(
            String projectId,
            String instanceId,
            String tableId,
            String columnFamily,
            String cellColumn,
            String messageClassName,
            RowType rowType,
            String rowKeyField,
            int startLookbackSeconds) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.tableId = tableId;
        this.columnFamily = columnFamily;
        this.cellColumn = cellColumn;
        this.messageClassName = messageClassName;
        this.rowType = rowType;
        this.rowKeyField = rowKeyField;
        this.startLookbackSeconds = startLookbackSeconds;
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
                BigtableChangeStreamSource source =
                        new BigtableChangeStreamSource(
                                projectId,
                                instanceId,
                                tableId,
                                columnFamily,
                                cellColumn,
                                messageClassName,
                                rowType,
                                rowKeyField,
                                startLookbackSeconds);

                return env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "bigtable-changestream-source")
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
                messageClassName,
                rowType,
                rowKeyField,
                startLookbackSeconds);
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "BigtableChangeStreamSource(project=%s, instance=%s, table=%s, family=%s, column=%s)",
                projectId, instanceId, tableId, columnFamily, cellColumn);
    }
}
