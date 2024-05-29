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

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Testing data generation logic. */
public class GMKLoadGeneratorTest {
    private Clock clock;
    private Random random;
    StreamExecutionEnvironment env;

    @Before
    public void setUp() {
        int totalSeconds = 7200;
        // Create a fixed Clock instance with the given number of seconds
        this.clock = Clock.fixed(Instant.ofEpochSecond(totalSeconds), ZoneId.systemDefault());
        this.random = new Random();
        // Avoid underterministic in test.
        random.setSeed(100);
        // configure test environment.
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.env.setParallelism(2);
    }

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
    public void testUnsupportedGenerator() throws Exception {
        // values are collected in a static variable
        CollectSink.VALUES.clear();

        // create a stream of custom elements and apply transformations
        this.env.fromData(1L, 2L, 3L, 4L)
                .filter(new InputLoadFilter(3600, "random", this.clock, this.random))
                .addSink(new CollectSink());

        // execute
        this.env.execute();

        // Fall back to static input pattern.
        assertTrue(CollectSink.VALUES.containsAll(Arrays.asList(1L, 2L, 3L, 4L)));
    }

    @Test
    public void testStaticGenerator() throws Exception {
        // values are collected in a static variable
        CollectSink.VALUES.clear();

        // create a stream of custom elements and apply transformations
        this.env.fromData(1L, 2L, 3L, 4L)
                .filter(new InputLoadFilter(3600, "static", this.clock, this.random))
                .addSink(new CollectSink());

        // execute
        this.env.execute();

        // test
        assertTrue(CollectSink.VALUES.containsAll(Arrays.asList(1L, 2L, 3L, 4L)));
    }

    @Test
    public void testSineGenerator() throws Exception {
        // values are collected in a static variable
        CollectSink.VALUES.clear();

        // create a stream of custom elements and apply transformations
        this.env.fromData(1L, 2L, 3L, 4L)
                .filter(new InputLoadFilter(3600, "sin", this.clock, this.random))
                .addSink(new CollectSink());

        // execute
        this.env.execute();

        // Passing probably is 50% when second % period == 0.
        assertTrue(CollectSink.VALUES.containsAll(Arrays.asList(3L, 4L)));
    }

    @Test
    public void testRampupGenerator() throws Exception {
        // values are collected in a static variable
        CollectSink.VALUES.clear();

        // create a stream of custom elements and apply transformations
        this.env.fromData(1L, 2L, 3L, 4L)
                .filter(new InputLoadFilter(3600, "rampup", this.clock, this.random))
                .addSink(new CollectSink());

        // execute
        this.env.execute();

        // Passing probably is 0% when second % period == 0.
        assertTrue(CollectSink.VALUES.isEmpty());
    }

    @Test
    public void testRampdownGenerator() throws Exception {
        // values are collected in a static variable
        CollectSink.VALUES.clear();

        // create a stream of custom elements and apply transformations
        this.env.fromData(1L, 2L, 3L, 4L)
                .filter(new InputLoadFilter(8000, "rampdown", this.clock, this.random))
                .addSink(new CollectSink());

        // execute
        this.env.execute();

        // Passing probably is 10%.
        assertTrue(CollectSink.VALUES.isEmpty());
    }
}
