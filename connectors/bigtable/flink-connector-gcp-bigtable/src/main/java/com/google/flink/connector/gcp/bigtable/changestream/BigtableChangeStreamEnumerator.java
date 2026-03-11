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
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.metrics.Counter;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

/**
 * Enumerates Bigtable Change Stream partitions and assigns them as splits to readers.
 *
 * <p>On {@link #start()}, discovers the initial partitions via the Bigtable API and distributes
 * them round-robin to available readers. Splits returned from failed readers are re-queued.
 */
public class BigtableChangeStreamEnumerator
        implements SplitEnumerator<
                BigtableChangeStreamSplit, Collection<BigtableChangeStreamSplit>> {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableChangeStreamEnumerator.class);

    private final SplitEnumeratorContext<BigtableChangeStreamSplit> context;
    private final String projectId;
    private final String instanceId;
    private final String tableId;
    private final Queue<BigtableChangeStreamSplit> pendingSplits;

    private transient BigtableDataClient client;

    // Metrics
    private final Counter partitionChangedEventsReceived;
    private final Counter splitsAssigned;
    private final Counter splitsAddedBack;
    private final Counter initialPartitionsDiscovered;
    private final Counter partitionChangedNoReaders;
    private final Counter unknownSourceEvents;

    public BigtableChangeStreamEnumerator(
            SplitEnumeratorContext<BigtableChangeStreamSplit> context,
            String projectId,
            String instanceId,
            String tableId,
            Collection<BigtableChangeStreamSplit> restoredSplits) {
        this.context = context;
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.tableId = tableId;
        this.pendingSplits = new ArrayDeque<>();
        if (restoredSplits != null) {
            this.pendingSplits.addAll(restoredSplits);
        }

        // Register metrics in constructor so they're available before start() and in tests
        partitionChangedEventsReceived =
                context.metricGroup().counter("partition_changed_events_received");
        splitsAssigned = context.metricGroup().counter("splits_assigned");
        splitsAddedBack = context.metricGroup().counter("splits_added_back");
        initialPartitionsDiscovered =
                context.metricGroup().counter("initial_partitions_discovered");
        partitionChangedNoReaders = context.metricGroup().counter("partition_changed_no_readers");
        unknownSourceEvents = context.metricGroup().counter("unknown_source_events");
        context.metricGroup().gauge("pending_splits", () -> pendingSplits.size());
    }

    @Override
    public void start() {
        try {
            BigtableDataSettings settings =
                    BigtableDataSettings.newBuilder()
                            .setProjectId(projectId)
                            .setInstanceId(instanceId)
                            .build();
            client = BigtableDataClient.create(settings);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create BigtableDataClient", e);
        }

        if (pendingSplits.isEmpty()) {
            List<ByteStringRange> partitions = new ArrayList<>();
            for (ByteStringRange partition :
                    client.generateInitialChangeStreamPartitions(tableId)) {
                partitions.add(partition);
            }
            LOG.info("Discovered {} initial partition(s) for table {}", partitions.size(), tableId);
            initialPartitionsDiscovered.inc(partitions.size());

            for (ByteStringRange partition : partitions) {
                pendingSplits.add(new BigtableChangeStreamSplit(partition, null));
            }
        } else {
            LOG.info("Restored {} split(s) from checkpoint", pendingSplits.size());
        }

        assignPendingSplits();
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        BigtableChangeStreamSplit split = pendingSplits.poll();
        if (split != null) {
            context.assignSplit(split, subtaskId);
            splitsAssigned.inc();
            LOG.info("Assigned split {} to subtask {}", split.splitId(), subtaskId);
        } else {
            LOG.debug("No pending splits for subtask {}", subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<BigtableChangeStreamSplit> splits, int subtaskId) {
        LOG.info("Adding {} split(s) back from subtask {}", splits.size(), subtaskId);
        splitsAddedBack.inc(splits.size());
        pendingSplits.addAll(splits);
        assignPendingSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("Reader {} registered", subtaskId);
        assignPendingSplits();
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof PartitionChangedEvent) {
            PartitionChangedEvent event = (PartitionChangedEvent) sourceEvent;
            partitionChangedEventsReceived.inc();
            List<BigtableChangeStreamSplit> newSplits = event.getNewSplits();
            LOG.info(
                    "Received PartitionChangedEvent from subtask {}: closedPartition={}, status={}, {} new split(s)",
                    subtaskId,
                    event.getClosedPartitionId(),
                    event.getCloseStreamStatus(),
                    newSplits.size());
            pendingSplits.addAll(newSplits);
            assignPendingSplits();
        } else {
            unknownSourceEvents.inc();
            LOG.warn("Received unknown source event: {}", sourceEvent.getClass().getName());
        }
    }

    @Override
    public Collection<BigtableChangeStreamSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
            LOG.info("Closed enumerator BigtableDataClient");
        }
    }

    /** Assigns at most one pending split per registered reader (round-robin). */
    private void assignPendingSplits() {
        int[] registeredReaders =
                context.registeredReaders().keySet().stream().mapToInt(Integer::intValue).toArray();

        if (registeredReaders.length == 0) {
            if (!pendingSplits.isEmpty()) {
                partitionChangedNoReaders.inc();
                LOG.warn("Pending splits exist but no readers are registered");
            }
            return;
        }

        // Assign one split per reader to avoid starving long-lived unbounded splits.
        // Remaining splits stay in pendingSplits and are assigned via handleSplitRequest.
        for (int subtaskId : registeredReaders) {
            BigtableChangeStreamSplit split = pendingSplits.poll();
            if (split == null) {
                break;
            }
            context.assignSplit(split, subtaskId);
            splitsAssigned.inc();
            LOG.info("Assigned split {} to subtask {}", split.splitId(), subtaskId);
        }

        if (!pendingSplits.isEmpty()) {
            LOG.info("{} split(s) remaining in pending queue", pendingSplits.size());
        }
    }
}
