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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;


/** GCS to GCS Wordcount using TableAPI. */
public class GCStoGCSTableApi {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String inputPath = params.get("input");
        String outputPath = params.get("output");
        Integer sinkMaxFileSizeMB = params.getInt("max-file-size-mb", 1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        final Schema schemaInput = Schema.newBuilder()
                .column("text", DataTypes.STRING())
                .build();

        final Schema schemaOutput = Schema.newBuilder()
                .column("word", DataTypes.STRING())
                .column("counted", DataTypes.BIGINT())
                .build();

        tableEnv.createTemporaryTable("words",
                TableDescriptor
                        .forConnector("filesystem")
                        .schema(schemaInput)
                        .option("path", inputPath)
                        .option("source.monitor-interval", "60s") // Check for new files every minute
                        .format(FormatDescriptor.forFormat("csv")
                            .option("header", "false")
                            .option("field-delimiter", "|")
                            .build())
                        .build());

        tableEnv.createTemporaryTable("wordcount",
                TableDescriptor
                        .forConnector("filesystem")
                        .schema(schemaOutput)
                        .option("path", outputPath)
                        .option("auto-compaction", "true")
                        .option("sink.rolling-policy.rollover-interval", "30s")
                        .option("compaction.file-size", sinkMaxFileSizeMB + "MB")
                        .format(FormatDescriptor.forFormat("canal-json").build())
                        .build());

        tableEnv.createTemporarySystemFunction("split", SplitWords.class);

        Table result = tableEnv.from("words")
            .flatMap(call("split", $("text"))).as("word")
            .groupBy($("word"))
            .select(
                $("word"),
                $("word").count().as("counted"));

        result.executeInsert("wordcount");
    }

    /** Split words. */
    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
    public static final class SplitWords extends TableFunction<Row> {
        public void eval(String sentence) {
            for (String split : sentence.split("[^\\p{L}]+")) {
                if (!split.equals(",") && !split.isEmpty()) {
                    collect(Row.of(split.toLowerCase()));
                }
            }
        }
    }
}
