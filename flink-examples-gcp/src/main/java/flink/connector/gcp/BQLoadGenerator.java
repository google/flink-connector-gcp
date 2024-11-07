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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProviderImpl;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.time.Clock;
import java.util.Random;

/** Pipeline code for running word count reading from Kafka and writing to BQ. */
public class BQLoadGenerator {
        static Schema schema;
        private static final int KB = 1024;
        public static void main(String[] args) throws Exception {
                final MultipleParameterTool parameters = MultipleParameterTool.fromArgs(args);
                String projectId = parameters.get("project-id");
                String datasetName = parameters.get("dataset-name");
                String tableName = parameters.get("table-name");
                String bqWordFieldName = parameters.get("bq-word-field-name", "word");
                String bqCountFieldName = parameters.get("bq-count-field-name", "countStr");
                String jobName = parameters.get("job-name", "BQ-Load-Gen");
                int load = parameters.getInt("messageSizeKB", 10);
                int rate = parameters.getInt("messagesPerSecond", 1000);
                Long maxRecords = parameters.getLong("max-records", 1_000_000_000L);
                String pattern = parameters.get("pattern", "static");
                Long loadPeriod = parameters.getLong("load-period-in-second", 3600);
                Long checkpointInterval = parameters.getLong("checkpoint-interval", 60000L);
                boolean wordGen = parameters.getBoolean("word-gen", true);
                boolean exactlyOnce = parameters.getBoolean("exactly-once", true);
                System.out.println("Starting job ".concat(jobName));
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.enableCheckpointing(checkpointInterval);
                // BQ sink requires checkpointing, which is enabled by default on Big Query Engine for Apache Flink
                // Enable checkpointing if trying to run with OSS Flink
                env.getConfig().setGlobalJobParameters(parameters);
                // BQ sink can only support up to 100 parallelism.
                env.getConfig().setMaxParallelism(100);

                GeneratorFunction<Long, Long> generatorFunction = n -> n;
                DataGeneratorSource<Long> generatorSource =
                        new DataGeneratorSource<>(
                                generatorFunction,
                                maxRecords,
                                RateLimiterStrategy.perSecond(rate),
                                Types.LONG);

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
                                .streamExecutionEnvironment(env)
                                .deliveryGuarantee(
                                        exactlyOnce
                                                ? DeliveryGuarantee.EXACTLY_ONCE
                                                : DeliveryGuarantee.AT_LEAST_ONCE)
                                .schemaProvider(schemaProvider)
                                .serializer(new AvroToProtoSerializer())
                                .build();

                DataStreamSource<Long> generator = env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(),
                "Data Generator");
                // Apply the input load filter.
                SingleOutputStreamOperator<Long> filteredGenerator = generator
                                .filter(new InputLoadFilter(loadPeriod, pattern, Clock.systemDefaultZone(), new Random()))
                                .uid(pattern.concat(" filter")).name("filtered load");

                if (wordGen) {
                        filteredGenerator
                                .flatMap(new WordLoadGenerator(load * KB))
                                .map(
                                        kv -> {
                                        GenericRecord rec =
                                                new GenericRecordBuilder(schemaProvider.getAvroSchema())
                                                        .set(bqWordFieldName, kv + "")
                                                        .set(bqCountFieldName, '1')
                                                        .build();
                                        return rec;
                                        })
                                .returns(
                                        new GenericRecordAvroTypeInfo(
                                                sinkConfig.getSchemaProvider().getAvroSchema()))
                                .sinkTo(BigQuerySink.get(sinkConfig)).uid("writer");
                }
                else {
                        filteredGenerator.map(
                                kv -> {
                                GenericRecord rec =
                                        new GenericRecordBuilder(schemaProvider.getAvroSchema())
                                                .set(bqWordFieldName, kv + "")
                                                .set(bqCountFieldName, '1')
                                                .build();
                                return rec;
                                })
                        .returns(
                                new GenericRecordAvroTypeInfo(
                                        sinkConfig.getSchemaProvider().getAvroSchema()))
                        .map(new FailingMapper()) // Fails on checkpoint with 20% probability
                        .returns(
                                new GenericRecordAvroTypeInfo(
                                        sinkConfig.getSchemaProvider().getAvroSchema()))
                        .sinkTo(BigQuerySink.get(sinkConfig)).uid("writer");
                }
                env.execute(jobName);
        }
}
