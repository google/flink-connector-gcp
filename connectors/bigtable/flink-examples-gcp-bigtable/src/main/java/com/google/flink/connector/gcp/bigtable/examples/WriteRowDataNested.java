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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.flink.connector.gcp.bigtable.BigtableSink;
import com.google.flink.connector.gcp.bigtable.serializers.RowDataToRowMutationSerializer;

/**
 * This is an example pipeline for Apache Flink that demonstrates writing data to Google Cloud
 * Bigtable using the Bigtable connector and a {@link RowDataToRowMutationSerializer} to transform
 * data into {@link RowMutationEntry} objects.
 *
 * <p>The pipeline generates a stream of Long values and uses a {@link
 * RowDataToRowMutationSerializer} to convert each Long value into a {@link RowMutationEntry} that
 * can be written to Bigtable.
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
public class WriteRowDataNested {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String instance = parameterTool.get("instance");
        String project = parameterTool.get("project");
        String table = parameterTool.get("table");
        Integer rate = parameterTool.getInt("rate", 100);
        String jobName = parameterTool.get("jobName", "Streaming Bigtable Write RowData Nested");

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

        DataType dtNestedSchema =
                DataTypes.ROW(
                        DataTypes.FIELD("my-key", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "family1",
                                DataTypes.ROW(
                                        DataTypes.FIELD("field-string", DataTypes.STRING()),
                                        DataTypes.FIELD("field-long", DataTypes.BIGINT()))),
                        DataTypes.FIELD(
                                "family2",
                                DataTypes.ROW(
                                        DataTypes.FIELD("field-string2", DataTypes.STRING()))));

        generator
                .map(new ToRowDataNested())
                .sinkTo(
                        BigtableSink.<RowData>builder()
                                .setProjectId(project)
                                .setInstanceId(instance)
                                .setTable(table)
                                .setSerializer(
                                        RowDataToRowMutationSerializer.builder()
                                                .withRowKeyField("my-key")
                                                .withSchema(dtNestedSchema)
                                                .withNestedRowsMode()
                                                .build())
                                .build())
                .name("BigtableSink");

        env.execute(jobName);
    }

    /** Convert to RowData. */
    public static class ToRowDataNested implements MapFunction<Long, RowData> {
        @Override
        public RowData map(Long l) throws Exception {
            GenericRowData r = new GenericRowData(3);
            String key = String.format("%d#%d#%d#%d", l % 11, l % 101, l % 1013, l);
            r.setField(0, StringData.fromString(key));

            GenericRowData family1 = new GenericRowData(2);
            family1.setField(0, StringData.fromString("field1#" + l));
            family1.setField(1, l);

            GenericRowData family2 = new GenericRowData(1);
            family2.setField(0, StringData.fromString("field2#" + l));

            r.setField(1, family1);
            r.setField(2, family2);
            return r;
        }
    }
}
