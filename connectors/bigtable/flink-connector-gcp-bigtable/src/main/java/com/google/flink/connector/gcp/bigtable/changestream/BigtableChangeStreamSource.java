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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.Collection;

/**
 * FLIP-27 Source that reads from Bigtable Change Streams and emits {@link RowData}.
 *
 * <p>Each Bigtable partition becomes a {@link BigtableChangeStreamSplit} assigned to a reader. The
 * reader deserializes cell value bytes using a pluggable {@link
 * RowKeyInjectingDeserializationSchema}.
 */
public class BigtableChangeStreamSource
        implements Source<
                        RowData, BigtableChangeStreamSplit, Collection<BigtableChangeStreamSplit>>,
                ResultTypeQueryable<RowData> {

    private final String projectId;
    private final String instanceId;
    private final String tableId;
    private final String columnFamily;
    private final String cellColumn;
    private final RowKeyInjectingDeserializationSchema deserializationSchema;
    private final int startLookbackSeconds;
    private final int bufferCapacity;
    private final int grpcChannelPoolSize;

    public BigtableChangeStreamSource(
            String projectId,
            String instanceId,
            String tableId,
            String columnFamily,
            String cellColumn,
            RowKeyInjectingDeserializationSchema deserializationSchema,
            int startLookbackSeconds,
            int bufferCapacity,
            int grpcChannelPoolSize) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.tableId = tableId;
        this.columnFamily = columnFamily;
        this.cellColumn = cellColumn;
        this.deserializationSchema = deserializationSchema;
        this.startLookbackSeconds = startLookbackSeconds;
        this.bufferCapacity = bufferCapacity;
        this.grpcChannelPoolSize = grpcChannelPoolSize;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<RowData, BigtableChangeStreamSplit> createReader(
            SourceReaderContext readerContext) {
        return new BigtableChangeStreamSourceReader(
                readerContext,
                projectId,
                instanceId,
                tableId,
                columnFamily,
                cellColumn,
                deserializationSchema,
                startLookbackSeconds,
                bufferCapacity,
                grpcChannelPoolSize);
    }

    @Override
    public SplitEnumerator<BigtableChangeStreamSplit, Collection<BigtableChangeStreamSplit>>
            createEnumerator(SplitEnumeratorContext<BigtableChangeStreamSplit> enumContext) {
        return new BigtableChangeStreamEnumerator(
                enumContext, projectId, instanceId, tableId, null);
    }

    @Override
    public SplitEnumerator<BigtableChangeStreamSplit, Collection<BigtableChangeStreamSplit>>
            restoreEnumerator(
                    SplitEnumeratorContext<BigtableChangeStreamSplit> enumContext,
                    Collection<BigtableChangeStreamSplit> checkpoint) {
        return new BigtableChangeStreamEnumerator(
                enumContext, projectId, instanceId, tableId, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<BigtableChangeStreamSplit> getSplitSerializer() {
        return BigtableChangeStreamSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<Collection<BigtableChangeStreamSplit>>
            getEnumeratorCheckpointSerializer() {
        return BigtableChangeStreamEnumeratorCheckpointSerializer.INSTANCE;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return InternalTypeInfo.of(deserializationSchema.getRowType());
    }
}
