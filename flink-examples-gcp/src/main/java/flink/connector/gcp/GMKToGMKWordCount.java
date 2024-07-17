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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.avro.Schema;

/** Pipeline code for running word count reading from GMK and writing to GMK. */
public class GMKToGMKWordCount {
    static Schema schema;

    public static void main(String[] args) throws Exception {
        final MultipleParameterTool parameters = MultipleParameterTool.fromArgs(args);
        String brokers = parameters.get("brokers", "localhost:9092");
        String gmkUsername = parameters.get("gmk-username", "");
        String kafkaTopic = parameters.get("kafka-topic", "my-topic");
        String kafkaSinkTopic = parameters.get("kafka-sink-topic", "sink-topic");
        boolean oauth = parameters.getBoolean("oauth", false);
        Long checkpointInterval = parameters.getLong("checkpoint-interval", 60000L);
        String jobName = parameters.get("job-name", "GMK-GMK-word-count");
        System.out.println("Starting job ".concat(jobName));
        System.out.println("Using SASL_SSL " + (oauth ? "OAUTHBEARER" : "PLAIN") + " to authenticate");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameters);
        env.enableCheckpointing(checkpointInterval);
        KafkaSourceBuilder<String> sourceBuilder = KafkaSource.<String>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(kafkaTopic)
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .setProperty("partition.discovery.interval.ms", "10000")
                    .setProperty("security.protocol", "SASL_SSL");

        KafkaSinkBuilder<String> sinkBuilder = KafkaSink.<String>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(kafkaSinkTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .setProperty("security.protocol", "SASL_SSL");
        if (oauth) {
            sourceBuilder.setProperty("sasl.mechanism", "OAUTHBEARER")
                        .setProperty("sasl.login.callback.handler.class", "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler")
                        .setProperty(
                                "sasl.jaas.config",
                                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
            sinkBuilder.setProperty("sasl.mechanism", "OAUTHBEARER")
                        .setProperty("sasl.login.callback.handler.class", "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler")
                        .setProperty(
                                "sasl.jaas.config",
                                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        } else {
            String config = "org.apache.kafka.common.security.plain.PlainLoginModule required"
            + " username=\'"
            + gmkUsername
            + "\'"
            + " password=\'"
            + System.getenv("GMK_PASSWORD")
                                            + "\';";
            sourceBuilder.setProperty("sasl.mechanism", "PLAIN")
                            .setProperty(
                                "sasl.jaas.config", config);

            sinkBuilder.setProperty("sasl.mechanism", "PLAIN")
                            .setProperty(
                                    "sasl.jaas.config", config);
        }
        KafkaSource<String> source = sourceBuilder.build();
        KafkaSink<String> sink = sinkBuilder.build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .flatMap(new PrepareWC())
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                .map(
                    tup -> {
                        return String.format("word: %s count: %s", tup.f0, tup.f1);
                    })
                .sinkTo(sink).name(jobName + "-sink");

        env.execute(jobName);
    }

    /** Splits tokens. */
    public static final class PrepareWC
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            for (String split : value.split(" ")) {
                if (!split.equals(",") && !split.isEmpty()) {
                    out.collect(new Tuple2<>(split.toLowerCase(), 1));
                }
            }
        }
    }
}
