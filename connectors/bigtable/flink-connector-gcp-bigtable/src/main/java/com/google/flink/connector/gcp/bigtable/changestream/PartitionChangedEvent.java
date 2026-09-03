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

import org.apache.flink.api.connector.source.SourceEvent;

import java.util.List;

/**
 * Sent from a reader to the enumerator when a {@code CloseStream} record is received, indicating
 * that the partition has split or merged. Carries the new continuation tokens that the enumerator
 * should convert into new splits and assign to readers.
 */
public class PartitionChangedEvent implements SourceEvent {

    private static final long serialVersionUID = 2L;

    private final List<BigtableChangeStreamSplit> newSplits;
    private final String closedPartitionId;
    private final String closeStreamStatus;

    public PartitionChangedEvent(
            List<BigtableChangeStreamSplit> newSplits,
            String closedPartitionId,
            String closeStreamStatus) {
        this.newSplits = newSplits;
        this.closedPartitionId = closedPartitionId;
        this.closeStreamStatus = closeStreamStatus;
    }

    /** Backward-compatible constructor for callers that don't have partition metadata. */
    public PartitionChangedEvent(List<BigtableChangeStreamSplit> newSplits) {
        this(newSplits, null, null);
    }

    public List<BigtableChangeStreamSplit> getNewSplits() {
        return newSplits;
    }

    public String getClosedPartitionId() {
        return closedPartitionId;
    }

    public String getCloseStreamStatus() {
        return closeStreamStatus;
    }
}
