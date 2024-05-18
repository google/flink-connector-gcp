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

/** Filter to convert static load to sine wave load. */
public class SineWaveFilter implements FilterFunction<String> {
    private long period = 3600; // load period for each sine wave in seconds
    private long currentTimeSeconds;

    public SineWaveFilter(long period) {
        this.period = period;
        this.currentTimeSeconds = Instant.now().getEpochSecond();
    }

    @Override
    public boolean filter(String value) throws Exception {
        // Started from a roughly const value.
        long seconds = Instant.now().getEpochSecond() - currentTimeSeconds;
        // Compute sin value.
        double sinRatePercentageToPassThrough =
            Math.sin(Math.toRadians((double) seconds / (double) this.period));
        double probOfPassing = Math.random();
        return sinRatePercentageToPassThrough > probOfPassing;
    }
}
