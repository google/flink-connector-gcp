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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;

import org.apache.avro.generic.GenericRecord;

/** Fails some % of the time after a checkpoint (20% by default).*/
public class FailingMapper
            implements MapFunction<GenericRecord, GenericRecord>, CheckpointListener {
    double odds;
    public FailingMapper(){
        odds = .2;
    }

    public FailingMapper(double customOdds){
        odds = customOdds;
    }

    @Override
    public GenericRecord map(GenericRecord value) {
        return value;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (Math.random() > odds) {
            throw new RuntimeException("Intentional failure in map");
        }
    }
}
