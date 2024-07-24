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
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.google.cloud.flink.bigquery.sink.serializer.BigQueryTableSchemaProvider;
import com.google.cloud.flink.bigquery.table.config.BigQuerySinkTableConfig;
import com.google.cloud.flink.bigquery.table.config.BigQueryTableConfig;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;


 /** BQ Wordcount using TableAPI. */
 public class BQTableAPI {

     public static void main(String[] args) throws Exception {
         ParameterTool params = ParameterTool.fromArgs(args);

         EnvironmentSettings settings = EnvironmentSettings
                 .newInstance()
                 .inStreamingMode()
                 .build();

         final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.getConfig().setGlobalJobParameters(params);

         StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

                 // create a table with example data without a connector required
        final Table rawWords =
        tableEnv.fromValues(
                Row.of("bar"),
                Row.of("foo"));

        final Table namedVals = rawWords.as("text");
        tableEnv.createTemporaryView("words", namedVals);

        // Declare Write Options.
        BigQueryTableConfig sinkTableConfig =
                BigQuerySinkTableConfig.newBuilder()
                        .table("test_table_table_2")
                        .project("managed-flink-shared-dev")
                        .dataset("test_table_ds")
                        .testMode(false)
                        .build();

        sinkTableConfig =
                BigQuerySinkTableConfig.newBuilder()
                        .table("test_table_table_2")
                        .project("managed-flink-shared-dev")
                        .dataset("test_table_ds")
                        .testMode(false)
                        .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();

        // Register the Sink Table
        tableEnv.createTable(
                "bigQuerySinkTable",
                BigQueryTableSchemaProvider.getTableDescriptor(sinkTableConfig));

         tableEnv.createTemporarySystemFunction("split", SplitWords.class);

         Table result = tableEnv.from("words")
             .flatMap(call("split", $("text"))).as("word");

         result.executeInsert("bigQuerySinkTable");
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
