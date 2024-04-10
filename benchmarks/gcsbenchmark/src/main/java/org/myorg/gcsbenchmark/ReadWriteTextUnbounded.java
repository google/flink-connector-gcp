package org.myorg.gcsbenchmark;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.stream.LongStream;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
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

public class ReadWriteTextUnbounded {
  private static final Logger LOG = LoggerFactory.getLogger(ReadWriteTextUnbounded.class);
  private static final int MB = 1024 * 1024;

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final ParameterTool parameters = ParameterTool.fromArgs(args);

    env.setParallelism(1);
    // This is not needed with AUTOMATIC detection, but to show this is STREAMING
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    env.disableOperatorChaining();
    env.getConfig().setGlobalJobParameters(parameters);

    String outputPath = parameters.get("output", "outputUnbounded");
    String outputPath2 = parameters.get("output2", "gs://chengedward-loadtest/finalOutput/");

    // Add checkpointing, this is needed for files to leave the "in progress state"
    Configuration config = new Configuration();
    config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
    env.enableCheckpointing(Duration.ofSeconds(5).toMillis());

    // Source 1 (Data Generator)
    GeneratorFunction<Long, Long> generatorFunction = n -> n;
    DataGeneratorSource<Long> generatorSource =
        new DataGeneratorSource<>(
            generatorFunction, Long.MAX_VALUE, RateLimiterStrategy.perSecond(1), Types.LONG);

    // Source 2 (Unbounded Read)
    FileSource<String> textUnboundedSource =
        FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(outputPath))
            .monitorContinuously(Duration.ofSeconds(30))
            .build();

    // Sink
    final FileSink<String> sink =
        FileSink.forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
            .withOutputFileConfig(
                OutputFileConfig.builder()
                    .withPartPrefix("RandStringBenchmark")
                    .withPartSuffix(".txt")
                    .build())
            .withRollingPolicy(
                DefaultRollingPolicy.builder().withRolloverInterval(Duration.ofSeconds(5)).build())
            .enableCompact(
                FileCompactStrategy.Builder.newBuilder()
                        .setSizeThreshold(100 * MB)
                        .enableCompactionOnCheckpoint(10)
                        .build(),
                new RecordWiseFileCompactor<>(
                        new DecoderBasedReader.Factory<>(SimpleStringDecoder::new)))
            .build();
    
    final FileSink<String> sink2 =
        FileSink.forRowFormat(new Path(outputPath2), new SimpleStringEncoder<String>("UTF-8"))
            .withOutputFileConfig(
                OutputFileConfig.builder()
                    .withPartPrefix("finalBenchmarkOutput")
                    .withPartSuffix(".txt")
                    .build())
            .withRollingPolicy(
                DefaultRollingPolicy.builder().withRolloverInterval(Duration.ofSeconds(5)).build())
            // .enableCompact(
            //     FileCompactStrategy.Builder.newBuilder()
            //             .setSizeThreshold(100 * MB)
            //             .enableCompactionOnCheckpoint(10)
            //             .build(),
            //     new RecordWiseFileCompactor<>(
            //             new DecoderBasedReader.Factory<>(SimpleStringDecoder::new)))
            .build();

    // Start Pipeline
    // Data Stream to Sink
    DataStreamSource<Long> generator =
        env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");

    generator.flatMap(new GCSLoadGenerator()).sinkTo(sink).uid("writer");

    // Continuously read the written stream
    DataStreamSource<String> readUnbounded =
        env.fromSource(textUnboundedSource, WatermarkStrategy.noWatermarks(), "Unbounded Read");

    readUnbounded.filter(s -> !s.isEmpty()).map(new AddTimeString()).sinkTo(sink2).uid("gcsToGcsWriter");

    // Execute
    env.execute("Read / Write to Text Unbounded");
  }

  public static final class GCSLoadGenerator implements FlatMapFunction<Long, String>{
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
      out.collect(randomStringOfSize(1 * MB));
    }
  }

//   public static final class FindDivisorsWithTime implements FlatMapFunction<Long, String> {

//     @Override
//     public void flatMap(Long value, Collector<String> out) {
//       Long sum = LongStream.rangeClosed(1, value + 1).filter(x -> value % x == 0).sum();
//       Date currentDate = new Date();
//       SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//       String currentDateTime = dateFormat.format(currentDate);
//       String s =
//           String.format(
//               "Number %d has divisor sum %d. Generated at %s", value, sum, currentDateTime);
//       out.collect(s);
//     }
//   }

  public static final class AddTimeString implements MapFunction<String, String> {
    @Override
    public String map(String element) throws Exception {
      Date currentDate = new Date();
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      String currentDateTime = dateFormat.format(currentDate);
      // int endInd = Math.min(element.length(), 10);
      return String.format("Unbounded read at %s of element\n\t\t==> %s", currentDateTime, element);
    }
  }
}
