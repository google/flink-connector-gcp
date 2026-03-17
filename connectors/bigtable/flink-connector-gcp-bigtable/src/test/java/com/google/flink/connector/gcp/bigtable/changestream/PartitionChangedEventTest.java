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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PartitionChangedEventTest {

    @Test
    void carriesNewSplitsFromPartitionSplit() {
        // Simulate a partition [a, z) splitting into [a, m) and [m, z)
        BigtableChangeStreamSplit left =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "m"), "token-left");
        BigtableChangeStreamSplit right =
                new BigtableChangeStreamSplit(ByteStringRange.create("m", "z"), "token-right");

        PartitionChangedEvent event = new PartitionChangedEvent(Arrays.asList(left, right));

        assertEquals(2, event.getNewSplits().size());
        assertEquals("token-left", event.getNewSplits().get(0).getContinuationToken());
        assertEquals("token-right", event.getNewSplits().get(1).getContinuationToken());
    }

    @Test
    void carriesNewSplitFromPartitionMerge() {
        // Simulate two partitions merging into one
        BigtableChangeStreamSplit merged =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "z"), "token-merged");

        PartitionChangedEvent event = new PartitionChangedEvent(Collections.singletonList(merged));

        assertEquals(1, event.getNewSplits().size());
        assertEquals("token-merged", event.getNewSplits().get(0).getContinuationToken());
    }

    @Test
    void carriesClosedPartitionMetadata() {
        BigtableChangeStreamSplit split =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "z"), "token");
        PartitionChangedEvent event =
                new PartitionChangedEvent(Collections.singletonList(split), "YQ==:eg==", "OK");

        assertEquals("YQ==:eg==", event.getClosedPartitionId());
        assertEquals("OK", event.getCloseStreamStatus());
        assertEquals(1, event.getNewSplits().size());
    }

    @Test
    void backwardCompatibleConstructorSetsNullMetadata() {
        BigtableChangeStreamSplit split =
                new BigtableChangeStreamSplit(ByteStringRange.create("a", "z"), "token");
        PartitionChangedEvent event = new PartitionChangedEvent(Collections.singletonList(split));

        assertNull(event.getClosedPartitionId());
        assertNull(event.getCloseStreamStatus());
        assertEquals(1, event.getNewSplits().size());
    }
}
