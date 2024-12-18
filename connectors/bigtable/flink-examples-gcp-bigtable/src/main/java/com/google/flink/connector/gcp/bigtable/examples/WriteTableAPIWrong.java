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

package com.google.flink.connector.gcp.bigtable.examples;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * This is an example pipeline for Apache Flink that demonstrates writing data to Google Cloud
 * Bigtable using the Bigtable connector and the Table API.
 *
 * <p>The pipeline generates a stream of rows with a string key, a string column, and an integer
 * column using the "datagen" connector. It then writes this data to Bigtable using the Table API
 * and the Bigtable connector.
 *
 * <p>To run this example, you need to pass the argument {@code --columnFamily} or have an existing
 * column family named {@code flink}.
 *
 * <p>You can run this example by passing the following command line arguments:
 *
 * <pre>
 *   --instance &lt;bigtable instance id&gt; \
 *   --project &lt;gcp project id&gt; \
 *   --table &lt;bigtable table id&gt; \
 *   --columnFamily &lt;bigtable column family id&gt; \
 *   --rate &lt;number of rows to generate per second&gt; \
 *   --jobName &lt;job name&gt;
 * </pre>
 */
public class WriteTableAPIWrong {
    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String instance = parameterTool.get("instance");
        String project = parameterTool.get("project");
        String table = parameterTool.get("table");
        String columnFamily = parameterTool.get("columnFamily", "flink");
        Long rate = parameterTool.getLong("rate", 100L);
        String jobName = parameterTool.get("jobName", "Streaming Bigtable Write Table API");

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.enableCheckpointing(parameterTool.getInt("interval", 1000));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        Schema schemaDatagen =
                Schema.newBuilder()
                        .column("my-key", DataTypes.STRING().notNull())
                        .column("stringColumn", DataTypes.STRING())
                        .column("intColumn", DataTypes.INT())
                        .build();

        Schema schemaBigtable =
                Schema.newBuilder()
                        .column("my-key", DataTypes.STRING().notNull())
                        .column("stringColumn", DataTypes.STRING())
                        .column("intColumn", DataTypes.INT())
                        .primaryKey("my-key")
                        .build();

        tableEnv.createTemporaryTable(
                "RandomGenerator",
                TableDescriptor.forConnector("datagen")
                        .schema(schemaDatagen)
                        .option(DataGenConnectorOptions.ROWS_PER_SECOND, rate)
                        .build());

        tableEnv.createTemporaryTable(
                "SinkBigtable",
                TableDescriptor.forConnector("bigtable")
                        .schema(schemaBigtable)
                        // .option(BigtableConnectorOptions.PROJECT, project)
                        // .option(BigtableConnectorOptions.INSTANCE, instance)
                        // .option(BigtableConnectorOptions.TABLE, table)
                        // .option(BigtableConnectorOptions.COLUMN_FAMILY, columnFamily)
                        .build());

        Table source = tableEnv.from("RandomGenerator");

        source.executeInsert("SinkBigtable");
    }
}
