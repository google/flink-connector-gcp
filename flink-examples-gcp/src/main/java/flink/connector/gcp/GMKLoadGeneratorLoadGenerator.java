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
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Clock;
import java.util.Random;

/** Pipeline code for generating load to Kafka. */
public class GMKLoadGeneratorLoadGenerator {
    private static final int KB = 1024;
    private static final int MB = 1024 * 1024;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool parameters = ParameterTool.fromArgs(args);
        String brokers = parameters.get("brokers", "localhost:9092");
        String kafkaUsername = parameters.get("kafka-username");
        String kafkaTopic = parameters.get("kafka-topic", "my-topic");
        int load = parameters.getInt("messageSizeKB", 10);
        int rate = parameters.getInt("messagesPerSecond", 1000);
        boolean oauth = parameters.getBoolean("oauth", true); // Only oauth is supported for Kafka for Big Query authentication
        Long maxRecords = parameters.getLong("max-records", 1_000_000_000L);
        Long loadPeriod = parameters.getLong("load-period-in-second", 3600);
        String pattern = parameters.get("pattern", "static");
        String jobName = parameters.get("job-name", "Kafka-load-gen");
        String project = parameters.get("project", "");
        String secretID = parameters.get("secret-id", "");
        String secretVersion = parameters.get("secret-version", "1");
        System.out.println("Starting job ".concat(jobName));
        System.out.println("Using SASL_SSL " + (oauth ? "OAUTHBEARER" : "PLAIN") + " to authenticate");

        env.getConfig().setGlobalJobParameters(parameters);

        // Source (Data Generator)
        GeneratorFunction<Long, Long> generatorFunction = n -> n;
        DataGeneratorSource<Long> generatorSource =
                new DataGeneratorSource<>(
                        generatorFunction,
                        maxRecords,
                        RateLimiterStrategy.perSecond(rate),
                        Types.LONG);

        KafkaSinkBuilder<String> sinkBuilder = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(kafkaTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setProperty("security.protocol", "SASL_SSL");
        if (oauth) {
                sinkBuilder.setProperty("sasl.mechanism", "OAUTHBEARER")
                                    .setProperty("sasl.login.callback.handler.class", "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler")
                                    .setProperty(
                                            "sasl.jaas.config",
                                            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        } else {
                String password = GetSecretVersion.getSecretVersionPayload(project, secretID, secretVersion);
                System.out.println("Got secret password for " + project + "/" + secretID + "/" + secretVersion);
                String config = "org.apache.kafka.common.security.plain.PlainLoginModule required"
                        + " username=\'"
                        + kafkaUsername
                        + "\'"
                        + " password=\'"
                        + password + "\';";
                sinkBuilder.setProperty("sasl.mechanism", "PLAIN")
                                .setProperty(
                                        "sasl.jaas.config", config);
        }
        KafkaSink<String> sink = sinkBuilder.build();
        DataStreamSource<Long> generator = env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(),
                        "Data Generator");
        // Apply the input load filter.
        SingleOutputStreamOperator<Long> filteredGenerator = generator
                        .filter(new InputLoadFilter(loadPeriod, pattern, Clock.systemDefaultZone(), new Random()))
                        .uid(pattern.concat(" filter")).name("filtered load");
        filteredGenerator.flatMap(new WordLoadGenerator(load * KB)).sinkTo(sink).uid("writer");
        // Execute
        env.execute(jobName);
}
}
