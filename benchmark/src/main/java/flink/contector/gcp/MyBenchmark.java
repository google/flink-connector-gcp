/*
 * Copyright (C) 2024 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package flink.contector.gcp;

import com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import flink.connector.gcp.ProtoSerializer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericData;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class MyBenchmark {
    private final Random random = new Random();
    private static final int NUMBER_OF_OPERATIONS = 100_000;

    private static final int RECORD_COUNT = 10;

    private final BigQuerySchemaProvider bigQuerySchemaProvider;

    private static BigQueryProtoSerializer serializer;
    private static BigQueryProtoSerializer fastSerializer;

    private GenericData.Record[] generatedRecords;
    private GenericData.Record generatedRecord;


    public MyBenchmark() {
        bigQuerySchemaProvider = TestBigQuerySchemas.getSchemaWithRequiredPrimitiveTypes();
    }

    public static void main(String[] args) throws RunnerException {
        org.openjdk.jmh.runner.options.Options opt = new OptionsBuilder()
            .include(MyBenchmark.class.getSimpleName())
            .addProfiler(GCProfiler.class)
            .build();
        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void prepare() throws Exception {
        serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);

        fastSerializer = new ProtoSerializer();

        // generate avro record
        generatedRecords = new GenericData.Record[RECORD_COUNT];
        for(int i = 0; i < RECORD_COUNT; i++){
            generatedRecords[i] = AvroRandomRecordGenerator
                .generateRandomRecordData(bigQuerySchemaProvider.getAvroSchema());
        }
        generatedRecord = generatedRecords[random.nextInt(RECORD_COUNT)];
    }

    @Benchmark
    @OperationsPerInvocation(NUMBER_OF_OPERATIONS)
    public void testFastAvroSerialization(Blackhole bh) throws Exception {
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            bh.consume(fastSerializer.serialize(generatedRecord));
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUMBER_OF_OPERATIONS)
    public void testAvroSerialization(Blackhole bh) throws Exception {
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            bh.consume(serializer.serialize(generatedRecord));
        }
    }
}
