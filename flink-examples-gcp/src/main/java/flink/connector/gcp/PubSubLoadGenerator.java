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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.pubsub.flink.PubSubSerializationSchema;
import com.google.pubsub.flink.PubSubSink;

import java.time.Clock;
import java.util.Random;

/** Creates load with pseudo random bytes and sends to the output PubSub topic. */
public class PubSubLoadGenerator {
    private static final int KB = 1024;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool parameters = ParameterTool.fromArgs(args);

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.getConfig().setGlobalJobParameters(parameters);

        String outputPath = parameters.get("output");
        String projectName = parameters.get("projectName");
        int load = parameters.getInt("messageSizeKB", 10);
        int rate = parameters.getInt("messagesPerSecond", 1000);
        Long loadPeriod = parameters.getLong("load-period-in-second", 3600);
        String pattern = parameters.get("pattern", "static");
        System.out.println(String.format("Message load: %d; Rate Per Sec: %d, Load pattern: %s, Load period: %d", load, rate, pattern, loadPeriod));

        // Source (Data Generator)
        GeneratorFunction<Long, Long> generatorFunction = n -> n;
        DataGeneratorSource<Long> generatorSource =
                new DataGeneratorSource<>(
                        generatorFunction,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(rate),
                        Types.LONG);

        // Sink
        final PubSubSink<String> sink =
                PubSubSink.<String>builder()
                        .setSerializationSchema(
                                PubSubSerializationSchema.dataOnly(
                                        new SimpleStringSchema()))
                        .setProjectName(projectName)
                        .setTopicName(outputPath)
                        .build();

        DataStreamSource<Long> generator =
                env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");

        // Apply the input load filter.
        SingleOutputStreamOperator<Long> filteredGenerator = generator.filter(new InputLoadFilter(loadPeriod, pattern, Clock.systemDefaultZone(), new Random())).uid(pattern.concat(" filter"));
        filteredGenerator.flatMap(new WordLoadGenerator(load * KB)).sinkTo(sink).uid("writer");

        env.execute("Write to PubSub Unbounded");
    }
}
