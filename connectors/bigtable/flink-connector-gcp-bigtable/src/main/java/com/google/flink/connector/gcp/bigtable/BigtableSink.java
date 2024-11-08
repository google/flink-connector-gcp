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

import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.flink.connector.gcp.bigtable.serializers.BaseRowMutationSerializer;
import com.google.flink.connector.gcp.bigtable.utils.CreateBigtableClients;
import com.google.flink.connector.gcp.bigtable.writer.BigtableFlushableWriter;
import com.google.flink.connector.gcp.bigtable.writer.BigtableSinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static <T> Builder<T> builder() {
        return new AutoValue_BigtableSink.Builder<T>();
    }

    private static final Logger logger = LoggerFactory.getLogger(BigtableSink.class);

    @Override
    public SinkWriter<T> createWriter(Sink.InitContext sinkInitContext) throws IOException {

        BigtableDataClient client =
                CreateBigtableClients.createDataClient(projectId(), instanceId());

        return new BigtableSinkWriter<T>(
                new BigtableFlushableWriter(client, sinkInitContext, table()), serializer());
    }

    /** Builder to create {@link BigtableSink}. */
    @AutoValue.Builder
    public abstract static class Builder<T> {
        public abstract Builder<T> setProjectId(String projectId);

        public abstract Builder<T> setInstanceId(String instanceId);

        public abstract Builder<T> setTable(String table);

        public abstract Builder<T> setSerializer(BaseRowMutationSerializer<T> serializer);

        public abstract BigtableSink<T> build();
    }
}
