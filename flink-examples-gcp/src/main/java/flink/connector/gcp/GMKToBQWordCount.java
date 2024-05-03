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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
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
        String bqWordFieldName = parameters.get("bq-word-field-name", "word");
        String bqCountFieldName = parameters.get("bq-count-field-name", "countStr");
        Long checkpointInterval = parameters.getLong("checkpoint-interval", 600000L);
        Integer bqSinkParallelism = parameters.getInt("bq-sink-parallelism", 5);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameters);
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                "gs://clairemccarthy-checkpoint/checkpoints/");
        env.configure(config);
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60000L);

        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers(brokers)
                        .setTopics(kafkaTopic)
                        .setGroupId("my-group-1")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setProperty("check.crcs", "false")
                        .setProperty("partition.discovery.interval.ms", "10000")
                        .setProperty("security.protocol", "SASL_SSL")
                        .setProperty("sasl.mechanism", "PLAIN")
                        .setProperty(
                                "sasl.jaas.config",
                                String.format(
                                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\'%s\' password=\"%s\";",
                                        gmkUsername, System.getenv("GMK_PASSWORD")))
                        .build();
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
                // .sum(1)
                .map(
                        kv -> {
                            GenericRecord rec =
                                    new GenericRecordBuilder(schemaProvider.getAvroSchema())
                                            .set(bqWordFieldName, kv.f0)
                                            .set(bqCountFieldName, kv.f1.toString())
                                            .build();
                            return rec;
                        }).setParallelism(bqSinkParallelism)
                .returns(
                        new GenericRecordAvroTypeInfo(
                                sinkConfig.getSchemaProvider().getAvroSchema()))
                .sinkTo(BigQuerySink.get(sinkConfig, env)).setParallelism(bqSinkParallelism);

        env.execute();
    }

    /**
     * Class for preparing word count.
     */
    public static final class PrepareWC
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String cleanStr = value.replaceAll("\\p{Punct}", " ");
            for (String split : cleanStr.split(" ")) {
                // if (isNumeric(split)) {
                //     continue;
                // }
                if (!split.isEmpty()) {
                    out.collect(new Tuple2<String, Integer>(split.toLowerCase(), 1));
                }
            }
        }

        public static boolean isNumeric(String str) {
            try {
                Double.parseDouble(str);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
    }
}
