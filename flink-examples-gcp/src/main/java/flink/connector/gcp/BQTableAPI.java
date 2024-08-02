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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.google.cloud.flink.bigquery.sink.serializer.BigQueryTableSchemaProvider;
import com.google.cloud.flink.bigquery.table.config.BigQuerySinkTableConfig;
import com.google.cloud.flink.bigquery.table.config.BigQueryTableConfig;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.lit;

/** BQ Wordcount using TableAPI. */
public class BQTableAPI {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String outputProject = params.get("output-project");
        String outputDataset = params.get("output-dataset");
        String outputTable = params.get("output-table");
        String brokers = params.get("brokers", "localhost:9092");
        String kafkaTopic = params.get("kafka-topic", "my-topic");

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(Duration.ofSeconds(5).toMillis());

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.getConfig().set("table.exec.source.idle-timeout", "5000");

        final Schema schemaInput = Schema.newBuilder()
                .column("text", DataTypes.STRING())
                .columnByMetadata("timestamp", DataTypes.TIMESTAMP_LTZ(3)).watermark("timestamp", $("timestamp").minus(lit(5).seconds()))
                .build();

        TableDescriptor.Builder sourceBuilder = TableDescriptor
            .forConnector("kafka")
            .schema(schemaInput)
            .option("topic", kafkaTopic)
            .option("properties.bootstrap.servers", brokers)
            .option("scan.startup.mode", "earliest-offset")
            .format(FormatDescriptor.forFormat("csv").build());
        sourceBuilder.option("properties.security.protocol", "SASL_SSL")
            .option("properties.sasl.mechanism", "OAUTHBEARER")
            .option("properties.sasl.login.callback.handler.class", "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler")
            .option("properties.sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");

        // Declare Write Options.
        BigQueryTableConfig sinkTableConfig = BigQuerySinkTableConfig.newBuilder()
                .table(outputTable)
                .project(outputProject)
                .dataset(outputDataset)
                .testMode(false)
                .build();
        // Register the Sink Table
        tableEnv.createTable(
            "bigQuerySinkTable",
            BigQueryTableSchemaProvider.getTableDescriptor(sinkTableConfig));

        tableEnv.createTemporaryTable("words",
                sourceBuilder.build());

        tableEnv.createTemporarySystemFunction("split", SplitWords.class);

        Table result = tableEnv.from("words")
            .select(call("split", $("text")).as("word"), $("timestamp").as("ts"))
            .window(Tumble.over(lit(1).minutes()).on($("ts")).as("w"))
            .groupBy($("w"), $("word"))
            .select(
                $("word"),
                $("word").count().as("counted"), $("w").start().as("window_start"), $("w").end().as("window_end"));

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
