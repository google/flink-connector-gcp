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
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.flink.connector.gcp.bigtable.BigtableSink;
import com.google.flink.connector.gcp.bigtable.serializers.GenericRecordToRowMutationSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;

/**
 * This is an example pipeline for Apache Flink that demonstrates writing data to Google Cloud
 * Bigtable using the Bigtable connector and a {@link GenericRecordToRowMutationSerializer} to
 * transform data into {@link RowMutationEntry} objects.
 *
 * <p>The pipeline generates a stream of Long values and uses a {@link
 * GenericRecordToRowMutationSerializer} to convert each Long value into a {@link RowMutationEntry}
 * that can be written to Bigtable.
 *
 * <p>To run this example, you need to pass the argument {@code --columnFamily} or have an existing
 * column family named {@code flink}.
 *
 * <p>You can run this example by passing the following command line arguments:
 *
 * <pre>
 *   --instance &lt;bigtable instance id&gt; \
 *   --project &lt;gcp project id&gt; \
 *   --table &lt;bigtable table id&gt; \
 *   --columnFamily &lt;bigtable column family id&gt; \
 *   --rate &lt;number of rows to generate per second&gt; \
 *   --jobName &lt;job name&gt;
 * </pre>
 */
public class WriteGenericRecord {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String instance = parameterTool.get("instance");
        String project = parameterTool.get("project");
        String table = parameterTool.get("table");
        String columnFamily = parameterTool.get("columnFamily", "flink");
        Integer rate = parameterTool.getInt("rate", 100);
        String jobName = parameterTool.get("jobName", "Streaming Bigtable Write GenericRecord");

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

        String schemaString =
                "{\"type\":\"record\", \"name\":\"User\", \"fields\":["
                        + "{\"name\":\"namekey\", \"type\":\"string\"},"
                        + "{\"name\":\"namefield\", \"type\":\"string\"},"
                        + "{\"name\":\"age\", \"type\":\"int\"},"
                        + "{\"name\":\"ageLong\", \"type\":\"long\"},"
                        + "{\"name\":\"ageDouble\", \"type\":\"double\"},"
                        + "{\"name\":\"ageFloat\", \"type\":\"float\"},"
                        + "{\"name\":\"textBytes\", \"type\":\"bytes\"},"
                        + "{\"name\":\"isActive\", \"type\":\"boolean\"}"
                        + "]}";

        Schema schema = new Schema.Parser().parse(schemaString);

        generator
                .map(new ToGenericRecord(schema))
                .returns(new GenericRecordAvroTypeInfo(schema))
                .sinkTo(
                        BigtableSink.<GenericRecord>builder()
                                .setProjectId(project)
                                .setInstanceId(instance)
                                .setTable(table)
                                .setSerializer(
                                        GenericRecordToRowMutationSerializer.builder()
                                                .withRowKeyField("namekey")
                                                .withColumnFamily(columnFamily)
                                                .build())
                                .build())
                .name("BigtableSink");

        env.execute(jobName);
    }

    /** Convert to GenericRecord. */
    public static final class ToGenericRecord implements MapFunction<Long, GenericRecord> {
        Schema schema;

        public ToGenericRecord(Schema schema) {
            this.schema = schema;
        }

        @Override
        public GenericRecord map(Long l) throws Exception {
            GenericRecord user = new GenericData.Record(schema);

            // Generate a non-lexicographically-sorted unique key
            String key = String.format("%d#%d#%d#%d", l % 11, l % 101, l % 1013, l);

            user.put("namekey", key);
            user.put("namefield", "field#" + l);
            user.put("age", l.intValue());
            user.put("ageLong", l);
            user.put("ageDouble", l.doubleValue());
            user.put("ageFloat", l.floatValue() / 3.3f);
            user.put("textBytes", ByteBuffer.wrap(key.getBytes()));
            user.put("isActive", l % 2 == 0);

            return user;
        }
    }
}
