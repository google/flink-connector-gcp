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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/** Pipeline code to trigger out of memory events. */
public class OOMJob {

  private static final Logger LOG = LoggerFactory.getLogger(OOMJob.class);

  private static final String JOB_NAME = OOMJob.class.getSimpleName();
  private static final String PARAM_MEM_ALLOC_MB = "memAllocMb";
  private static final int MiB = 1024 * 1024;
  private static final int DEFAULT_MEM_ALLOC_MB = 1024;

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ParameterTool params = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(params);

    int memAllocMb = params.getInt(PARAM_MEM_ALLOC_MB, DEFAULT_MEM_ALLOC_MB);

    DataStreamSource<Long> read = env.fromSource(new NumberSequenceSource(1L, 10L),
        WatermarkStrategy.noWatermarks(), "Read");

    read.flatMap(new FlatMapFunction<Long, Void>() {
      private List<byte[]> memAlloc = null;

      @Override
      public void flatMap(Long value, Collector<Void> out) {
        LOG.info("Handling value: {}", value);
        if (memAlloc == null) {
          LOG.info("Allocating memory {} MB", memAllocMb);
          memAlloc = new ArrayList<>();
          for (int i = 0; i < memAllocMb; ++i) {
            byte[] block = new byte[MiB];
            // Arrays.fill(block, (byte) -1);
            memAlloc.add(block);
            Runtime runtime = Runtime.getRuntime();
            LOG.info("Allocated {} MB so far. maxMemory: {}, totalMemory: {}, freeMemory: {}",
                i, runtime.maxMemory(), runtime.totalMemory(), runtime.freeMemory());
          }
          LOG.info("Memory allocated successfully.");
        }
        LOG.info("Done for value: {}", value);
      }
    });

    env.execute(String.format("%s:%dMB", JOB_NAME, memAllocMb));
  }
}
