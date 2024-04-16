package flink.connector.gcp;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.compactor.DecoderBasedReader;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
import org.apache.flink.connector.file.sink.compactor.RecordWiseFileCompactor;
import org.apache.flink.connector.file.sink.compactor.SimpleStringDecoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Creates load with pseudo random bytes and sends to the output GCS bucket.
 */
public class GCSLoadGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(GCSLoadGenerator.class);
    private static final int KB = 1024;
    private static final int MB = 1024 * 1024;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool parameters = ParameterTool.fromArgs(args);

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.getConfig().setGlobalJobParameters(parameters);

        String outputPath = parameters.get("output", "gs://source/");
        int load = parameters.getInt("kbload", 1000);
        int rate = parameters.getInt("ratePerSecond", 100);
        int paralellism = parameters.getInt("parallelism", 100);
        System.out.println(
                String.format(
                        "Message load: %d; Rate Per Sec: %d; Paralellism: %d",
                        load, rate, paralellism));
        env.setParallelism(paralellism);

        // Add checkpointing, this is needed for files to leave the "in progress state"
        Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        env.enableCheckpointing(Duration.ofSeconds(5).toMillis());

        // Source 1 (Data Generator)
        GeneratorFunction<Long, Long> generatorFunction = n -> n;
        DataGeneratorSource<Long> generatorSource =
                new DataGeneratorSource<>(
                        generatorFunction,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(rate),
                        Types.LONG);

        // Sink
        final FileSink<String> sink =
                FileSink.forRowFormat(
                                new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                        .withOutputFileConfig(
                                OutputFileConfig.builder()
                                        .withPartPrefix("RandStringBenchmark")
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

        DataStreamSource<Long> generator =
                env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");

        generator.flatMap(new LoadGenerator(load * KB)).sinkTo(sink).uid("writer");

        env.execute("Write to Text Unbounded");
    }

    /**
     * Creates load with pseudo random bytes.
     */
    public static final class LoadGenerator implements FlatMapFunction<Long, String> {
        int load;

        public LoadGenerator(int l) {
            load = l;
        }

        static final byte[] RANDOM_STR =
                ("13VhL5nTJp1SvToxTcFeNdBdDKpw6Js3dFwXgohhRifPyLw3Zaf5wXihLC9EkLEhyyozxbPAg0OsFSvPkaQXxSKKS"
                                + "WZWqvp0F54vudCAAPsVSaMTnARHcTqk21pLQM0Hc5NaenbSEcVCC3tZMxs6gVLBOKziQX9qC4Wh1DKyS4HWap"
                                + "ITlOyltRedxLPZgIVBX1EiCaAAg3ULGY6Bkstl7oZUSYf1LzbE24WpnKgIA0IzaOIPNn7ATS8esGm8KIFU22p"
                                + "Ac9LIDkt9yp5rBMyLfuYbRZ9iw6mZ7QsHmVMozuhEOFB0dOL7fDqnnKmZcFcmfYzUs9m0knoBdh0HsMg4IUg9"
                                + "fkRQqQgYOQRh6ekf15Kl0GV7yPJrPjfSAXuQCkvIunOmeQsYDkZ13VhL5nTJp1SvToxTcFeNdBdDKpw6Js3dF"
                                + "XgohhRifPyLw3Zaf5wXihLC9EkLEhyyozxbPAg0OsFSvPkaQXxSKKSFrWZWqvp0F54vudCAAPsVSaMTnARHcT"
                                + "NOuWqk21pLQM0Hc5NaenbSEcVCC3tZMxs6gVLBOKziQX9qC4Wh1DKyS4HWapITlOyltRedxLPZgIVBX1EiCaA"
                                + "Ag3ULGY6Bkstl7oZUSYf1LzbE24WpnKgIA0IzaOIPNn7ATS8esGm8KIFU22pNAc9LIDkt9yp5rBMyLfuYbRZ9"
                                + "iw6mZ7QsHmVMozuhEOFB0dOL7fDqnnKmZcFcmfYzUs9m0knoBdh0HsMg4IUg9oUfkRQqQgYOQRh6ekf15Kl0G"
                                + "V7yPJrPjfSAXuQCkvIunOmeQsYDkZ")
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
