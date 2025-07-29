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

package com.google.flink.connector.gcp.bigtable.utils;

import com.google.api.gax.batching.BatchingSettings;
import org.apache.flink.FlinkVersion;

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;

import javax.annotation.Nullable;

import java.io.IOException;

/** Utily class to create Bigtable Clients. */
public class CreateBigtableClients {

    private static final HeaderProvider USER_AGENT_HEADER_PROVIDER =
            FixedHeaderProvider.create(
                    "user-agent", "flink-bigtable-connector/" + FlinkVersion.current().toString());

    /** Creates Data client used for writing. */
    public static BigtableDataClient createDataClient(
            String project,
            String instance,
            Boolean flowControl,
            @Nullable String appProfileId,
            @Nullable GoogleCredentials credentials,
            long batchSize)
            throws IOException {
        BigtableDataSettings.Builder bigtableBuilder = BigtableDataSettings.newBuilder();
        bigtableBuilder.setProjectId(project).setInstanceId(instance);
        bigtableBuilder.setBulkMutationFlowControl(flowControl);
        BatchingSettings batchingSettings = bigtableBuilder.stubSettings().bulkMutateRowsSettings().getBatchingSettings();
        bigtableBuilder.stubSettings().bulkMutateRowsSettings().setBatchingSettings(
                batchingSettings
                        .toBuilder()
                        .setElementCountThreshold(batchSize)
                        .build());

        bigtableBuilder
                .stubSettings()
                .setQuotaProjectId(project)
                .setHeaderProvider(USER_AGENT_HEADER_PROVIDER);

        if (appProfileId != null) {
            bigtableBuilder.setAppProfileId(appProfileId);
        }

        if (credentials != null) {
            bigtableBuilder.setCredentialsProvider(() -> credentials);
        }

        return BigtableDataClient.create(bigtableBuilder.build());
    }
}
