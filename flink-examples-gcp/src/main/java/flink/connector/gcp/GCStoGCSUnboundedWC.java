package flink.connector.gcp;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
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
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool parameters = ParameterTool.fromArgs(args);

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.getConfig().setGlobalJobParameters(parameters);

        String inputPath = parameters.get("input");
        String outputPath = parameters.get("output", "gs://output/");
        // Add checkpointing, this is needed for files to leave the "in progress state"
        Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        env.enableCheckpointing(Duration.ofSeconds(5).toMillis());

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
                .filter(s -> !s.isEmpty())
                .map(new AddTimeString())
                .flatMap(new PrepareWC())
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                .map(kv -> String.format("Word: %s Count: %s", kv.f0, kv.f1))
                .sinkTo(sink)
                .uid("gcsToGcsWriter");

        // Execute
        env.execute("Read / Write to Text Unbounded");
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
            for (String split : value.split("[^\\p{L}]+")) {
                if (!split.equals(",") && !split.isEmpty()) {
                    out.collect(new Tuple2<>(split.toLowerCase(), 1));
                }
            }
        }
    }
}
