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

package com.google.flink.connector.gcp.bigtable.changestream;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class BigtableChangeStreamSplitSerializerTest {

    private final BigtableChangeStreamSplitSerializer serializer =
            BigtableChangeStreamSplitSerializer.INSTANCE;

    @Test
    void roundTripWithToken() throws IOException {
        ByteStringRange partition =
                ByteStringRange.create(
                        ByteString.copyFromUtf8("abc"), ByteString.copyFromUtf8("xyz"));
        BigtableChangeStreamSplit original =
                new BigtableChangeStreamSplit(partition, "my-token-123");

        byte[] bytes = serializer.serialize(original);
        BigtableChangeStreamSplit deserialized =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(original.getPartition(), deserialized.getPartition());
        assertEquals("my-token-123", deserialized.getContinuationToken());
    }

    @Test
    void roundTripWithoutToken() throws IOException {
        ByteStringRange partition = ByteStringRange.create("start", "end");
        BigtableChangeStreamSplit original = new BigtableChangeStreamSplit(partition, null);

        byte[] bytes = serializer.serialize(original);
        BigtableChangeStreamSplit deserialized =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(original.getPartition(), deserialized.getPartition());
        assertNull(deserialized.getContinuationToken());
    }

    @Test
    void roundTripWithEmptyPartitionBoundaries() throws IOException {
        ByteStringRange partition = ByteStringRange.create(ByteString.EMPTY, ByteString.EMPTY);
        BigtableChangeStreamSplit original = new BigtableChangeStreamSplit(partition, "tok");

        byte[] bytes = serializer.serialize(original);
        BigtableChangeStreamSplit deserialized =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(ByteString.EMPTY, deserialized.getPartition().getStart());
        assertEquals(ByteString.EMPTY, deserialized.getPartition().getEnd());
        assertEquals("tok", deserialized.getContinuationToken());
    }
}
