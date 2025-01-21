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

package com.google.flink.connector.gcp.bigtable;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.flink.connector.gcp.bigtable.serializers.FunctionRowMutationSerializer;
import com.google.flink.connector.gcp.bigtable.testingutils.TestingUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the {@link BigtableSink} class.
 *
 * <p>This class verifies the functionality of the {@link BigtableSink} by testing its builder and
 * ensuring that the provided parameters are correctly set.
 */
public class BigtableSinkTest {

    @Test
    public void testCorrectBigtableSinkInitialization() {
        FunctionRowMutationSerializer<RowMutationEntry> serializer =
                new FunctionRowMutationSerializer<RowMutationEntry>(t -> t);

        BigtableSink<RowMutationEntry> sink =
                BigtableSink.<RowMutationEntry>builder()
                        .setProjectId(TestingUtils.PROJECT)
                        .setInstanceId(TestingUtils.INSTANCE)
                        .setSerializer(serializer)
                        .setTable(TestingUtils.TABLE)
                        .setFlowControl(true)
                        .setAppProfileId(TestingUtils.APP_PROFILE)
                        .build();

        assertEquals(TestingUtils.PROJECT, sink.projectId());
        assertEquals(TestingUtils.INSTANCE, sink.instanceId());
        assertEquals(TestingUtils.TABLE, sink.table());
        assertEquals(TestingUtils.APP_PROFILE, sink.appProfileId());
        assertTrue(sink.flowControl());
        assertEquals(serializer, sink.serializer());
    }

    @Test
    public void testCorrectDefaultBigtableSinkInitialization() {
        FunctionRowMutationSerializer<RowMutationEntry> serializer =
                new FunctionRowMutationSerializer<RowMutationEntry>(t -> t);

        BigtableSink<RowMutationEntry> sink =
                BigtableSink.<RowMutationEntry>builder()
                        .setProjectId(TestingUtils.PROJECT)
                        .setInstanceId(TestingUtils.INSTANCE)
                        .setSerializer(serializer)
                        .setTable(TestingUtils.TABLE)
                        .build();

        assertEquals(TestingUtils.PROJECT, sink.projectId());
        assertEquals(TestingUtils.INSTANCE, sink.instanceId());
        assertEquals(TestingUtils.TABLE, sink.table());
        assertFalse(sink.flowControl());
        assertEquals(serializer, sink.serializer());
        assertNull(sink.appProfileId());
    }
}
