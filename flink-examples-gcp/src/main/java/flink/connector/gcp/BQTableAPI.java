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

package flink.connector.gcp;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.google.cloud.flink.bigquery.sink.serializer.BigQueryTableSchemaProvider;
import com.google.cloud.flink.bigquery.table.config.BigQuerySinkTableConfig;
import com.google.cloud.flink.bigquery.table.config.BigQueryTableConfig;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

 /** Generates data and writes it to BQ using TableAPI. */
 public class BQTableAPI {

     public static void main(String[] args) throws Exception {
         ParameterTool params = ParameterTool.fromArgs(args);
         String projectId = params.get("project-id");
         String datasetName = params.get("dataset-name");
         String tableName = params.get("table-name");
         String rowsPerSec = params.get("rows-per-second");

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(Duration.ofSeconds(5).toMillis());

         StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

         final Schema schemaInput = Schema.newBuilder()
                .column("text", DataTypes.STRING())
                .build();

        TableDescriptor.Builder sourceBuilder = TableDescriptor
            .forConnector("datagen").schema(schemaInput).option("rows-per-second", rowsPerSec);

        // Declare Write Options.
        BigQueryTableConfig sinkTableConfig =
                BigQuerySinkTableConfig.newBuilder()
                        .table(tableName)
                        .project(projectId)
                        .dataset(datasetName)
                        .testMode(false)
                        .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();

        // Register the Sink Table
        tableEnv.createTable(
            "bigQuerySinkTable",
            BigQueryTableSchemaProvider.getTableDescriptor(sinkTableConfig));

        tableEnv.createTemporaryTable("words",
                sourceBuilder.build());

        tableEnv.createTemporarySystemFunction("split", SplitWords.class);

         Table result = tableEnv.from("words")
             .flatMap(call("split", $("text"))).as("word");

        result.executeInsert("bigQuerySinkTable");
    }

    /** Split words. */
    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
    public static final class SplitWords extends TableFunction<Row> {
        public void eval(String sentence) {
            for (String split : sentence.split("[^\\p{L}]+")) {
                if (!split.equals(",") && !split.isEmpty()) {
                    collect(Row.of(split.toLowerCase()));
                }
            }
        }
    }
}
