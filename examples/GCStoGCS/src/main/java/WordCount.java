import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount {

  private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final ParameterTool parameters = ParameterTool.fromArgs(args);
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    env.getConfig().setGlobalJobParameters(parameters);

    String inputPath = parameters.get("input", "gs://apache-beam-samples/shakespeare/kinglear.txt");
    String outputPath = parameters.get("output", "outputBounded");
    Integer parallelism = parameters.getInt("parallelism", 1);

    env.setParallelism(parallelism);

    // Source
    FileSource<String> source =
        FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(inputPath)).build();

    final FileSink<String> sink =
        FileSink.forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
            .withOutputFileConfig(
                OutputFileConfig.builder()
                    .withPartPrefix("bounded_write")
                    .withPartSuffix(".txt")
                    .build())
            .build();

    //  Start Pipeline
    DataStreamSource<String> read =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Read files");

    read.flatMap(new PrepareWC())
        .keyBy(tuple -> tuple.f0)
        .sum(1)
        .map(kv -> String.format("Word: %s Count: %s", kv.f0, kv.f1))
        .sinkTo(sink)
        .uid("writer");

    // Execute
    env.execute("WordCount");
  }

  public static final class PrepareWC implements FlatMapFunction<String, Tuple2<String, Integer>> {

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
