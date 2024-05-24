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

import org.apache.flink.api.common.functions.FilterFunction;

import java.time.Instant;

/** Filter to convert static load to different load patterns. */
public class InputLoadFilter implements FilterFunction<Long> {
    private String pattern = "static"; // sin, static.
    private long period = 3600; // load period for each sine wave in seconds

    public InputLoadFilter(long period, String pattern) {
        this.period = period;
        this.pattern = pattern;
    }

    @Override
    public boolean filter(Long value) throws Exception {
        // Started from a roughly const value.
        long seconds = Instant.now().getEpochSecond();
        double ratePercentageToPassThrough = 1;
        if (this.pattern.equals("sin")) {
            ratePercentageToPassThrough = 0.5 * Math.sin(2 * Math.PI * seconds / this.period) + 0.5;
        }
        double probOfPassing = Math.random();
        return ratePercentageToPassThrough > probOfPassing;
    }
}

// @AutoBuilder(ofClass = InputLoadFilter.class)
