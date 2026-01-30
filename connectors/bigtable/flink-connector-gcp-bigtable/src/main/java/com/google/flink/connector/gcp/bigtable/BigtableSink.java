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

package com.google.flink.connector.gcp.bigtable;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.flink.connector.gcp.bigtable.serializers.BaseRowMutationSerializer;
import com.google.flink.connector.gcp.bigtable.utils.CreateBigtableClients;
import com.google.flink.connector.gcp.bigtable.writer.BigtableFlushableWriter;
import com.google.flink.connector.gcp.bigtable.writer.BigtableSinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Google Cloud Bigtable sink to write messages to a Cloud Bigtable Instance
 *
 * <p>{@link BigtableSink} is constructed and configured using {@link Builder}. {@link BigtableSink}
 * cannot be configured after it is built. See {@link Builder} for how {@link BigtableSink} can be
 * configured.
 */
@AutoValue
public abstract class BigtableSink<T> implements Sink<T> {

    public abstract String projectId();

    public abstract String instanceId();

    public abstract String table();

    public abstract BaseRowMutationSerializer<T> serializer();

    public abstract Boolean flowControl();

    public abstract @Nullable String appProfileId();

    public abstract @Nullable GoogleCredentials credentials();

    public abstract @Nullable Long batchSize();

    public static <T> Builder<T> builder() {
        return new AutoValue_BigtableSink.Builder<T>().setFlowControl(false);
    }

    private static final Logger logger = LoggerFactory.getLogger(BigtableSink.class);

    @Override
    public SinkWriter<T> createWriter(WriterInitContext sinkInitContext) throws IOException {
        BigtableDataClient client =
                CreateBigtableClients.createDataClient(
                        projectId(),
                        instanceId(),
                        flowControl(),
                        appProfileId(),
                        credentials(),
                        batchSize());

        return new BigtableSinkWriter<T>(
                new BigtableFlushableWriter(client, sinkInitContext, table()),
                serializer(),
                sinkInitContext);
    }

    /** Builder to create {@link BigtableSink}. */
    @AutoValue.Builder
    public abstract static class Builder<T> {
        /** The project id of the Google Cloud Bigtable instance. */
        public abstract Builder<T> setProjectId(String projectId);

        /** The instance id of the Google Cloud Bigtable. */
        public abstract Builder<T> setInstanceId(String instanceId);

        /** The table id of the Google Cloud Bigtable instance. */
        public abstract Builder<T> setTable(String table);

        /**
         * The serializer to serialize the incoming records into {@link
         * com.google.cloud.bigtable.data.v2.models.RowMutation}.
         */
        public abstract Builder<T> setSerializer(BaseRowMutationSerializer<T> serializer);

        /**
         * Sets the <a href="https://cloud.google.com/bigtable/docs/writes#flow-control">batch flow
         * control</a> parameter for writing. Optional, defaults to `False`.
         */
        public abstract Builder<T> setFlowControl(Boolean flowControl);

        /**
         * Sets the <a href="https://cloud.google.com/bigtable/docs/app-profiles">app profile id</a>
         * parameter for writing. Optional.
         */
        public abstract Builder<T> setAppProfileId(String appProfileId);

        /** Google Credentials for Bigtable. Optional. */
        public abstract Builder<T> setCredentials(GoogleCredentials credentials);

        /** The number of elements to group in a batch. * */
        public abstract Builder<T> setBatchSize(long batchSize);

        public abstract BigtableSink<T> build();
    }
}
