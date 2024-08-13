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
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProviderImpl;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

/** Pipeline code for running word count reading from GMK and writing to BQ. */
public class GMKToBQWordCount {
    static Schema schema;

    public static void main(String[] args) throws Exception {
        final MultipleParameterTool parameters = MultipleParameterTool.fromArgs(args);
        String brokers = parameters.get("brokers", "localhost:9092");
        String gmkUsername = parameters.get("gmk-username");
        String kafkaTopic = parameters.get("kafka-topic", "my-topic");
        String projectId = parameters.get("project-id");
        String datasetName = parameters.get("dataset-name");
        String tableName = parameters.get("table-name");
        boolean oauth = parameters.getBoolean("oauth", false);
        String bqWordFieldName = parameters.get("bq-word-field-name", "word");
        String bqCountFieldName = parameters.get("bq-count-field-name", "countStr");
        String kafkaGroupId = parameters.get("kafka-group-id", "kafka-source-of-".concat(tableName));
        String jobName = parameters.get("job-name", "GMK-BQ-word-count");
        System.out.println("Starting job ".concat(jobName).concat(" with Kafka group id: ".concat(kafkaGroupId)));
        System.out.println("Using SASL_SSL " + (oauth ? "OAUTHBEARER" : "PLAIN") + " to authenticate");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameters);
        // BQ sink can only support up to 100 parallelism.
        env.getConfig().setMaxParallelism(100);

        KafkaSourceBuilder<String> sourceBuilder = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(kafkaTopic)
                .setGroupId(kafkaGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "10000")
                .setProperty("security.protocol", "SASL_SSL");
        if (oauth) {
                sourceBuilder.setProperty("sasl.mechanism", "OAUTHBEARER")
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
        }
        KafkaSource<String> source = sourceBuilder.build();

        BigQueryConnectOptions sinkConnectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId(projectId)
                        .setDataset(datasetName)
                        .setTable(tableName)
                        .build();
        BigQuerySchemaProvider schemaProvider = new BigQuerySchemaProviderImpl(sinkConnectOptions);
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(sinkConnectOptions)
                        .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .schemaProvider(schemaProvider)
                        .serializer(new AvroToProtoSerializer())
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .flatMap(new PrepareWC())
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                .map(
                        kv -> {
                            GenericRecord rec =
                                    new GenericRecordBuilder(schemaProvider.getAvroSchema())
                                            .set(bqWordFieldName, kv.f0)
                                            .set(bqCountFieldName, kv.f1.toString())
                                            .build();
                            return rec;
                        })
                .returns(
                        new GenericRecordAvroTypeInfo(
                                sinkConfig.getSchemaProvider().getAvroSchema()))
                .sinkTo(BigQuerySink.get(sinkConfig, env));

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
