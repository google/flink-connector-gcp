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
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
 import org.apache.flink.table.functions.TableFunction;
 import org.apache.flink.types.Row;

 import com.google.cloud.flink.bigquery.sink.serializer.BigQueryTableSchemaProvider;
 import com.google.cloud.flink.bigquery.table.config.BigQuerySinkTableConfig;
 import com.google.cloud.flink.bigquery.table.config.BigQueryTableConfig;

 import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.lit;

  /** BQ Wordcount using TableAPI. */
  public class BQTableAPI {

      public static void main(String[] args) throws Exception {
          ParameterTool params = ParameterTool.fromArgs(args);
          String outputTable = params.get("output-table");
          String brokers = params.get("brokers", "localhost:9092");
          String kafkaTopic = params.get("kafka-topic", "my-topic");

          EnvironmentSettings settings = EnvironmentSettings
                  .newInstance()
                  .inStreamingMode()
                  .build();

          final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.getConfig().setGlobalJobParameters(params);

          StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

         final Schema schemaInput = Schema.newBuilder()
                 .column("text", DataTypes.STRING())
                 .columnByMetadata("timestamp", DataTypes.TIMESTAMP(3))
                 .build();

         TableDescriptor.Builder sourceBuilder = TableDescriptor
             .forConnector("kafka")
             .schema(schemaInput)
             .option("topic", kafkaTopic)
             .option("properties.bootstrap.servers", brokers)
             .option("scan.startup.mode", "earliest-offset")
             .format(FormatDescriptor.forFormat("csv")
                 .option("field-delimiter", "|")
                 .build());
        sourceBuilder.option("properties.security.protocol", "SASL_SSL")
                 .option("properties.sasl.mechanism", "OAUTHBEARER")
                 .option("properties.sasl.login.callback.handler.class", "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler")
                 .option("properties.sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
                  // Declare Write Options.
         BigQueryTableConfig sinkTableConfig = BigQuerySinkTableConfig.newBuilder()
                 .table(outputTable)
                 .project("managed-flink-shared-dev")
                 .dataset("test_table_ds")
                 .testMode(false)
                 .build();
        // Register the Sink Table
         tableEnv.createTable(
             "bigQuerySinkTable",
             BigQueryTableSchemaProvider.getTableDescriptor(sinkTableConfig));

        tableEnv.createTemporaryTable("words",
                sourceBuilder.build());

        tableEnv.createTemporarySystemFunction("split", SplitWords.class);

        Table result = tableEnv.from("words")
            .flatMap(call("split", $("text"))).as("word")
            .window(Tumble.over(lit(1).hours()).on($("timestamp")).as("hourlyWindow"))
            .groupBy($("hourlyWindow"), $("word"))
            .select(
                $("word"),
                $("word").count().as("counted"));

        result.executeInsert("bigQuerySinkTable");

        //  // execute a Flink SQL job and print the result locally
        //  tableEnv.sqlQuery(
        //      // define the aggregation
        //      "SELECT word, SUM(frequency) AS `count`\n"
        //              // read from an artificial fixed-size table with rows and columns
        //              + "FROM (\n"
        //              + "  VALUES ('Hello', 1, CURRENT_TIME), ('Ciao', 1, CURRENT_TIME)\n"
        //              + ")\n"
        //              // name the table and its columns
        //              + "AS WordTable(word, frequency, ts)\n"
        //              // group for aggregation
        //              + "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND), word").executeInsert("bigQuerySinkTable");

         // final Schema schemaInput = Schema.newBuilder()
         //         .column("text", DataTypes.STRING())
         //         .columnByExpression("proc_time", "PROCTIME()")
         //         .build();

         // tableEnv.createTemporaryTable("words",
         //         TableDescriptor
         //                 .forConnector("filesystem")
         //                 .schema(schemaInput)
         //                 .option("path", inputPath)
         //                 .option("source.monitor-interval", "60s") // Check for new files every minute
         //                 .format(FormatDescriptor.forFormat("csv")
         //                     .option("header", "false")
         //                     .option("field-delimiter", "|")
         //                     .build())
         //                 .build());

         // tableEnv.createTemporarySystemFunction("split", SplitWords.class);

         // Table result = tableEnv.from("words")
         //     .window(Tumble.over(lit(1).minutes()).on($("proc_time"))).as("w")
         //     .flatMap(call("split", $("text"))).as("word")
         //     .groupBy($("word"), $("w"))
         //     .select($("w"),
         //         $("word"),
         //         $("word").count().as("counted"));

         // // create a table with example data without a connector required
         // final Table rawWords =
         // tableEnv.fromValues(DataTypes.ROW(
         //     DataTypes.FIELD("text", DataTypes.STRING()),
         //     DataTypes.FIELD("rowtime", DataTypes.TIMESTAMP_LTZ())),
         //         Row.of("bar",  LocalDateTime.parse("2021-11-16T08:19:30.123")),
         //         Row.of("foo", LocalDateTime.parse("2021-11-16T08:19:30.123")));

         // final Table namedVals = rawWords.as("text", "rowtime");
         // tableEnv.createTemporaryView("words", namedVals);

         // sinkTableConfig =
         //         BigQuerySinkTableConfig.newBuilder()
         //                 .table("test_table_table")
         //                 .project("managed-flink-shared-dev")
         //                 .dataset("test_table_ds")
         //                 .testMode(false)
         //                 .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
         //                 .build();

         // tableEnv.createTemporarySystemFunction("split", SplitWords.class);

         // Table wordCounts = rawWords.window(Tumble.over(lit(1).minutes()).on($("rowtime").rowtime()).as("foo")).groupBy($("text"), $("foo").cast(DataTypes.TIMESTAMP())).select(
         //     $("text"),
         //     $("text").count().as("counted"));
         // wordCounts.executeInsert("bigQuerySinkTable");

         //  Table result = tableEnv.from("words").select($("text").as("word"), $("rowtime"))
         //     //  .flatMap(call("split", $("text"))).as("word")
         //      .window(Tumble.over(lit(1).hours()).on($("rowtime")).as("ts"))
         //      .groupBy($("ts").cast(DataTypes.TIMESTAMP_LTZ()))
         //      .select(
         //          $("word"),
         //          $("word").count().as("counted"));

         //  result.executeInsert("bigQuerySinkTable");
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
