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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class BigtableChangeStreamSplitTest {

    @Test
    void freshSplitHasNullToken() {
        ByteStringRange partition = ByteStringRange.create("a", "z");
        BigtableChangeStreamSplit split = new BigtableChangeStreamSplit(partition, null);

        assertNull(split.getContinuationToken());
        assertNotNull(split.getPartition());
    }

    @Test
    void withTokenReturnsNewSplitWithSamePartition() {
        ByteStringRange partition = ByteStringRange.create("a", "z");
        BigtableChangeStreamSplit original = new BigtableChangeStreamSplit(partition, null);

        BigtableChangeStreamSplit updated = original.withToken("token-1");

        assertNull(original.getContinuationToken());
        assertEquals("token-1", updated.getContinuationToken());
        assertEquals(original.getPartition(), updated.getPartition());
    }

    @Test
    void splitIdIsStableForSamePartition() {
        ByteStringRange partition =
                ByteStringRange.create(
                        ByteString.copyFromUtf8("start"), ByteString.copyFromUtf8("end"));
        BigtableChangeStreamSplit s1 = new BigtableChangeStreamSplit(partition, null);
        BigtableChangeStreamSplit s2 = new BigtableChangeStreamSplit(partition, "some-token");

        assertEquals(s1.splitId(), s2.splitId());
    }

    @Test
    void splitIdDiffersForDifferentPartitions() {
        BigtableChangeStreamSplit s1 =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "m"), null);
        BigtableChangeStreamSplit s2 =
                new BigtableChangeStreamSplit(ByteStringRange.create("m", "z"), null);

        assertNotNull(s1.splitId());
        assertNotNull(s2.splitId());
        assert (!s1.splitId().equals(s2.splitId()));
    }
}
