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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/** Pipeline code for generating load to GCS. */
public class GMKLoadGenerator {
    private static final int KB = 1024;
    private static final int MB = 1024 * 1024;

        public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        String brokers = parameters.get("brokers", "localhost:9092");
        String gmkUsername = parameters.get("gmk-username");
        String kafkaTopic = parameters.get("kafka-topic", "my-topic");
        int load = parameters.getInt("kbload", 10);
        double rate = parameters.getDouble("rate", 1000);
        Long maxRecords = parameters.getLong("max-records", 1_000_000_000L);

        env.getConfig().setGlobalJobParameters(parameters);

        // Source (Data Generator)
        GeneratorFunction<Long, Long> generatorFunction = n -> n;
        DataGeneratorSource<Long> generatorSource =
                new DataGeneratorSource<>(
                        generatorFunction,
                        maxRecords,
                        RateLimiterStrategy.perSecond(rate),
                        Types.LONG);

        KafkaSink<String> sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers(brokers)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(kafkaTopic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setProperty("security.protocol", "SASL_SSL")
                        .setProperty("sasl.mechanism", "PLAIN")
                        .setProperty(
                                "sasl.jaas.config",
                                "org.apache.kafka.common.security.plain.PlainLoginModule required"
                                        + " username=\'"
                                        + gmkUsername
                                        + "\'"
                                        + " password=\'"
                                        + System.getenv("GMK_PASSWORD")
                                        + "\';")
                        .build();

        DataStreamSource<Long> generator =
                        env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");
        generator.flatMap(new LoadGenerator(load * KB)).sinkTo(sink).uid("writer");
        // Execute
        env.execute();
    }

    /** Creates load with a Shakespeare passage. */
    public static final class LoadGenerator implements FlatMapFunction<Long, String> {
        int load;

        public LoadGenerator(int l) {
            load = l;
        }

        static final byte[] RANDOM_STR =
                ("To be, or not to be, that is the question:"
                + "Whether 'tis nobler in the mind to suffer"
                + "The slings and arrows of outrageous fortune,"
                + "Or to take arms against a sea of troubles"
                + "And by opposing end them. To die—to sleep,"
                + "No more; and by a sleep to say we end"
                + "The heart-ache and the thousand natural shocks"
                + "That flesh is heir to: 'tis a consummation"
                + "Devoutly to be wish'd. To die, to sleep;"
                + "To sleep, perchance to dream—ay, there's the rub:"
                + "For in that sleep of death what dreams may come,"
                + "When we have shuffled off this mortal coil,"
                + "Must give us pause—there's the respect"
                + "That makes calamity of so long life."
                + "For who would bear the whips and scorns of time,"
                + "Th'oppressor's wrong, the proud man's contumely,"
                + "The pangs of dispriz'd love, the law's delay,"
                + "The insolence of office, and the spurns"
                + "That patient merit of th'unworthy takes,"
                + "When he himself might his quietus make"
                + "With a bare bodkin? Who would fardels bear,"
                + "To grunt and sweat under a weary life,"
                + "But that the dread of something after death,"
                + "The undiscovere'd country, from whose bourn"
                + "No traveller returns, puzzles the will,"
                + "And makes us rather bear those ills we have"
                + "Than fly to others that we know not of?"
                + "Thus conscience doth make cowards of us all,"
                + "And thus the native hue of resolution"
                + "Is sicklied o'er with the pale cast of thought,"
                + "And enterprises of great pith and moment"
                + "With this regard their currents turn awry"
                + "And lose the name of action.")
                        .getBytes();

        public static byte[] makePseudoRandomBytes(int sizeBytes) {
            // since it the bytes will be converted with UTF-16 encoding
            sizeBytes = sizeBytes / 2;
            byte[] result = new byte[sizeBytes];
            int current = 0;
            int remaining = sizeBytes;
            while (remaining > 0) {
                int len = Math.min(RANDOM_STR.length, remaining);
                System.arraycopy(RANDOM_STR, 0, result, current, len);
                current += len;
                remaining -= len;
            }
            return result;
        }

        public String randomStringOfSize(int sizeBytes) {
            return new String(makePseudoRandomBytes(sizeBytes));
        }

        @Override
        public void flatMap(Long value, Collector<String> out) {
            out.collect(randomStringOfSize(load));
        }
    }
}
