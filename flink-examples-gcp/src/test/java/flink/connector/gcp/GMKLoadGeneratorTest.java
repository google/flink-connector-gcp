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

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Testing data generation logic. */
public class GMKLoadGeneratorTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(1)
                    .setNumberTaskManagers(5)
                    .build());

    // create a testing sink
    private static class CollectSink implements SinkFunction<Long> {

        // must be static
        public static final List<Long> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Long value, SinkFunction.Context context) throws Exception {
            VALUES.add(value);
        }
    }

    @Test
    public void testSinGenerator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);
        // values are collected in a static variable
        CollectSink.VALUES.clear();

        // create a stream of custom elements and apply transformations
        env.fromData(1L, 2L, 3L)
                .filter(new InputLoadFilter(7200, "sin"))
                .addSink(new CollectSink());

        // execute
        env.execute();

        // test
        assertFalse(CollectSink.VALUES.containsAll(Arrays.asList(1L, 2L, 3L)));
    }

    @Test
    public void testStaticGenerator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);
        // values are collected in a static variable
        CollectSink.VALUES.clear();

        // create a stream of custom elements and apply transformations
        env.fromData(1L, 2L, 3L)
                .filter(new InputLoadFilter(7200, "static"))
                .addSink(new CollectSink());

        // execute
        env.execute();

        // test
        assertTrue(CollectSink.VALUES.containsAll(Arrays.asList(1L, 2L, 3L)));
    }
}
