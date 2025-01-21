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
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.flink.connector.gcp.bigtable.BigtableSink;
import com.google.flink.connector.gcp.bigtable.serializers.RowDataToRowMutationSerializer;
import com.google.flink.connector.gcp.bigtable.table.config.BigtableConnectorOptions;
import com.google.flink.connector.gcp.bigtable.utils.CredentialsFactory;
import com.google.flink.connector.gcp.bigtable.utils.ErrorMessages;

import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@link DynamicTableSink} for writing data into a Google Cloud Bigtable table. It generates the
 * Sink for Table API.
 */
public class BigtableDynamicTableSink implements DynamicTableSink {
    protected final Integer parallelism;
    protected final ReadableConfig connectorOptions;
    protected final ResolvedSchema resolvedSchema;
    protected final String rowKeyField;

    public BigtableDynamicTableSink(
            ResolvedSchema resolvedSchema, ReadableConfig connectorOptions) {
        checkArgument(
                resolvedSchema.getPrimaryKeyIndexes().length == 1,
                String.format(
                        ErrorMessages.MULTIPLE_PRIMARY_KEYS_TEMPLATE,
                        resolvedSchema.getPrimaryKeyIndexes().length));
        int rowKeyIndex = resolvedSchema.getPrimaryKeyIndexes()[0];
        checkArgument(
                resolvedSchema
                        .getColumn(rowKeyIndex)
                        .get()
                        .getDataType()
                        .equals(DataTypes.STRING().notNull()),
                String.format(
                        ErrorMessages.ROW_KEY_STRING_TYPE_TEMPLATE,
                        resolvedSchema.getColumn(rowKeyIndex).get().getDataType()));

        this.connectorOptions = connectorOptions;
        this.resolvedSchema = resolvedSchema;
        this.parallelism = connectorOptions.get(BigtableConnectorOptions.SINK_PARALLELISM);
        this.rowKeyField = resolvedSchema.getColumn(rowKeyIndex).get().getName();
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.resolvedSchema, this.connectorOptions);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        BigtableDynamicTableSink other = (BigtableDynamicTableSink) obj;
        return Objects.equals(resolvedSchema, other.resolvedSchema)
                && Objects.equals(connectorOptions, other.connectorOptions);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataType physicalSchema = resolvedSchema.toPhysicalRowDataType();

        RowDataToRowMutationSerializer.Builder serializerBuilder =
                RowDataToRowMutationSerializer.builder()
                        .withSchema(physicalSchema)
                        .withRowKeyField(this.rowKeyField);

        if (connectorOptions.get(BigtableConnectorOptions.USE_NESTED_ROWS_MODE)) {
            serializerBuilder.withNestedRowsMode();
        } else {
            serializerBuilder.withColumnFamily(
                    connectorOptions.get(BigtableConnectorOptions.COLUMN_FAMILY));
        }

        BigtableSink.Builder<RowData> sinkBuilder =
                BigtableSink.<RowData>builder()
                        .setProjectId(connectorOptions.get(BigtableConnectorOptions.PROJECT))
                        .setTable(connectorOptions.get(BigtableConnectorOptions.TABLE))
                        .setInstanceId(connectorOptions.get(BigtableConnectorOptions.INSTANCE))
                        .setFlowControl(connectorOptions.get(BigtableConnectorOptions.FLOW_CONTROL))
                        .setSerializer(serializerBuilder.build());

        if (connectorOptions.getOptional(BigtableConnectorOptions.APP_PROFILE_ID).isPresent()) {
            sinkBuilder.setAppProfileId(
                    connectorOptions.get(BigtableConnectorOptions.APP_PROFILE_ID));
        }

        Optional<GoogleCredentials> credentials =
                CredentialsFactory.builder()
                        .setAccessToken(
                                connectorOptions.get(
                                        BigtableConnectorOptions.CREDENTIALS_ACCESS_TOKEN))
                        .setCredentialsFile(
                                connectorOptions.get(BigtableConnectorOptions.CREDENTIALS_FILE))
                        .setCredentialsKey(
                                connectorOptions.get(BigtableConnectorOptions.CREDENTIALS_KEY))
                        .build()
                        .getCredentialsOr();

        if (credentials.isPresent()) {
            sinkBuilder.setCredentials(credentials.get());
        }

        final BigtableSink<RowData> bigtableSink = sinkBuilder.build();

        if (parallelism == null) {
            return SinkV2Provider.of(bigtableSink);
        }
        return SinkV2Provider.of(bigtableSink, parallelism);
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "BigtableSink("
                        + "parallelism=%s, "
                        + "connectorOptions=%s, "
                        + "resolvedSchema=%s, "
                        + "rowKeyField=%s)",
                parallelism, connectorOptions, resolvedSchema, rowKeyField);
    }

    @Override
    public DynamicTableSink copy() {
        return new BigtableDynamicTableSink(resolvedSchema, connectorOptions);
    }
}
