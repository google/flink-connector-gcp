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

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/** Pipeline code for running word count reading from GMK and writing to GMK. */
public class GMKToGMKTableApi {
    static Schema schema;

    public static void main(String[] args) throws Exception {
        final MultipleParameterTool parameters = MultipleParameterTool.fromArgs(args);
        String brokers = parameters.get("brokers", "localhost:9092");
        String gmkUsername = parameters.get("gmk-username");
        String kafkaTopic = parameters.get("kafka-topic", "my-topic");
        String kafkaSinkTopic = parameters.get("kafka-sink-topic", "sink-topic");
        boolean oauth = parameters.getBoolean("oauth", false);
        Long checkpointInterval = parameters.getLong("checkpoint-interval", 60000L);
        String jobName = parameters.get("job-name", "GMK-GMK-word-count");
        System.out.println("Starting job ".concat(jobName));
        System.out.println("Using SASL_SSL " + (oauth ? "OAUTHBEARER" : "PLAIN") + " to authenticate");
        Configuration conf = new Configuration();
        conf.setString("restart-strategy.type", "fixed-delay");
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .withConfiguration(conf)
                .build();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.getConfig().setGlobalJobParameters(parameters);
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofMillis(10000L));
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000L);
        env.getCheckpointConfig().setCheckpointTimeout(600000L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        env.getConfig().setUseSnapshotCompression(true);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        final Schema schemaInput = Schema.newBuilder()
                .column("text", DataTypes.STRING())
                .build();

        final Schema schemaOutput = Schema.newBuilder()
                .column("word", DataTypes.STRING())
                .column("counted", DataTypes.BIGINT())
                .build();

        TableDescriptor.Builder sourceBuilder = TableDescriptor
            .forConnector("kafka")
            .schema(schemaInput)
            .option("topic", kafkaTopic)
            .option("properties.bootstrap.servers", brokers)
            .option("scan.startup.mode", "earliest-offset")
            .format(FormatDescriptor.forFormat("csv")
                .option("field-delimiter", "|")
                .build());

        TableDescriptor.Builder sinkBuilder = TableDescriptor
            .forConnector("kafka")
            .schema(schemaOutput)
            .option("topic", kafkaSinkTopic)
            .option("properties.bootstrap.servers", brokers)
            .option("scan.startup.mode", "earliest-offset")
            .format(FormatDescriptor.forFormat("canal-json").build());

        if (oauth) {
            sourceBuilder.option("properties.security.protocol", "SASL_SSL")
                .option("properties.sasl.mechanism", "OAUTHBEARER")
                .option("properties.sasl.login.callback.handler.class", "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler")
                .option("properties.sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
            sinkBuilder.option("properties.security.protocol", "SASL_SSL")
                .option("properties.sasl.mechanism", "OAUTHBEARER")
                .option("properties.sasl.login.callback.handler.class", "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler")
                .option("properties.sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        } else {
            String config = "org.apache.kafka.common.security.plain.PlainLoginModule required"
                + " username=\'"
                + gmkUsername
                + "\'"
                + " password=\'"
                + System.getenv("GMK_PASSWORD")
                + "\';";
            sourceBuilder.option("properties.security.protocol", "SASL_SSL")
                .option("properties.sasl.mechanism", "PLAIN")
                .option("properties.sasl.jaas.config", config);
            sinkBuilder.option("properties.security.protocol", "SASL_SSL")
                .option("properties.sasl.mechanism", "PLAIN")
                .option("properties.sasl.jaas.config", config);
            }

        tableEnv.createTemporaryTable("words",
                sourceBuilder.build());

        tableEnv.createTemporaryTable("wordcount",
                sinkBuilder.build());

        tableEnv.createTemporarySystemFunction("split", SplitWords.class);

        Table result = tableEnv.from("words")
            .flatMap(call("split", $("text"))).as("word")
            .groupBy($("word"))
            .select(
                $("word"),
                $("word").count().as("counted"));

        result.executeInsert("wordcount");

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
