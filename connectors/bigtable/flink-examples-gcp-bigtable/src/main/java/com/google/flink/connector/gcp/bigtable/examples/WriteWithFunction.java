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

package com.google.flink.connector.gcp.bigtable.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.function.SerializableFunction;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.flink.connector.gcp.bigtable.BigtableSink;
import com.google.flink.connector.gcp.bigtable.serializers.FunctionRowMutationSerializer;

/**
 * This is an example pipeline for Apache Flink that demonstrates writing data to Google Cloud
 * Bigtable using the Bigtable connector and a {@link FunctionRowMutationSerializer} to transform
 * data into {@link RowMutationEntry} objects.
 *
 * <p>The pipeline generates a stream of Long values and uses a {@link
 * FunctionRowMutationSerializer} to convert each Long value into a {@link RowMutationEntry} that
 * can be written to Bigtable.
 *
 * <p>To run this example, your Bigtable table needs to have column families {@code div3} and {@code
 * not-div3}.
 *
 * <p>You can run this example by passing the following command line arguments:
 *
 * <pre>
 *   --instance &lt;bigtable instance id&gt; \
 *   --project &lt;gcp project id&gt; \
 *   --table &lt;bigtable table id&gt; \
 *   --rate &lt;number of rows to generate per second&gt; \
 *   --jobName &lt;job name&gt;
 * </pre>
 */
public class WriteWithFunction {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String instance = parameterTool.get("instance");
        String project = parameterTool.get("project");
        String table = parameterTool.get("table");
        Integer rate = parameterTool.getInt("rate", 10000);
        String jobName = parameterTool.get("jobName", "Streaming Bigtable Write With Function");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        GeneratorFunction<Long, Long> generatorFunction = n -> n;
        DataGeneratorSource<Long> generatorSource =
                new DataGeneratorSource<>(
                        generatorFunction,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(rate),
                        Types.LONG);

        DataStreamSource<Long> generator =
                env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");

        SerializableFunction<Long, RowMutationEntry> toMutationEntry =
                (l -> {
                    // Generate a non-lexicographically-sorted unique key
                    String key = String.format("%d#%d#%d#%d", l % 11, l % 101, l % 1013, l);
                    String family = l % 3 == 0 ? "div3" : "not-div3";
                    String qualifier = String.valueOf(l % 10);
                    RowMutationEntry entry =
                            RowMutationEntry.create(key)
                                    .setCell(family, qualifier, "my-value " + l);
                    return entry;
                });

        generator
                .sinkTo(
                        BigtableSink.<Long>builder()
                                .setProjectId(project)
                                .setInstanceId(instance)
                                .setTable(table)
                                .setSerializer(
                                        new FunctionRowMutationSerializer<Long>(toMutationEntry))
                                .build())
                .name("BigtableSink");

        env.execute(jobName);
    }
}
