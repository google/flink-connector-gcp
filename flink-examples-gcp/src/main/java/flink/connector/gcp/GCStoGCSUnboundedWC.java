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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.compactor.DecoderBasedReader;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
import org.apache.flink.connector.file.sink.compactor.RecordWiseFileCompactor;
import org.apache.flink.connector.file.sink.compactor.SimpleStringDecoder;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/** Pipeline read from and writing to GCS buckets. */
public class GCStoGCSUnboundedWC {
    private static final Logger LOG = LoggerFactory.getLogger(GCStoGCSUnboundedWC.class);
    private static final int KB = 1024;
    private static final int MB = 1024 * 1024;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("restart-strategy.type", "fixed-delay");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        final ParameterTool parameters = ParameterTool.fromArgs(args);

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.getConfig().setGlobalJobParameters(parameters);

        String inputPath = parameters.get("input");
        String outputPath = parameters.get("output", "gs://output/");
        String checkpointDir = parameters.get("checkpoint");
        String jobName = parameters.get("job-name", "GCS-GCS-word-count");

        // Add checkpointing, this is needed for files to leave the "in progress state"
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
        env.configure(config);
        env.enableCheckpointing(10000L);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofMillis(10000L));
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000L);
        env.getCheckpointConfig().setCheckpointTimeout(600000L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        env.getConfig().setUseSnapshotCompression(true);

        // Source (Unbounded Read)
        FileSource<String> textUnboundedSource =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(inputPath))
                        .monitorContinuously(Duration.ofSeconds(30))
                        .build();

        // Sink
        final FileSink<String> sink =
                FileSink.forRowFormat(
                                new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                        .withOutputFileConfig(
                                OutputFileConfig.builder()
                                        .withPartPrefix("GCStoGCSUnbounded")
                                        .withPartSuffix(".txt")
                                        .build())
                        .withRollingPolicy(
                                DefaultRollingPolicy.builder()
                                        .withRolloverInterval(Duration.ofSeconds(5))
                                        .build())
                        .enableCompact(
                                FileCompactStrategy.Builder.newBuilder()
                                        .setSizeThreshold(20 * MB)
                                        .enableCompactionOnCheckpoint(10)
                                        .build(),
                                new RecordWiseFileCompactor<>(
                                        new DecoderBasedReader.Factory<>(SimpleStringDecoder::new)))
                        .build();

        // Start Pipeline
        // Continuously read the written stream
        DataStreamSource<String> readUnbounded =
                env.fromSource(
                        textUnboundedSource, WatermarkStrategy.noWatermarks(), "Unbounded Read");

        readUnbounded
                .flatMap(new PrepareWC())
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                .map(kv -> String.format("Word: %s Count: %s", kv.f0, kv.f1))
                .sinkTo(sink)
                .uid("gcsToGcsWriter");

        // Execute
        env.execute(jobName);
    }

    /** Prepends date and time before message. */
    public static final class AddTimeString implements MapFunction<String, String> {
        @Override
        public String map(String element) throws Exception {
            Date currentDate = new Date();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String currentDateTime = dateFormat.format(currentDate);
            return String.format(
                    "Unbounded read at %s of element\n\t\t==> %s", currentDateTime, element);
        }
    }

    /** Splits tokens. */
    public static final class PrepareWC
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            for (String split : value.split(" ")) {
                if (!split.equals(",") && !split.isEmpty()) {
                    out.collect(new Tuple2<>(split.toLowerCase(), 1));
                }
            }
        }
    }
}
