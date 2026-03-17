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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Enumerates Bigtable Change Stream partitions and assigns them as splits to readers.
 *
 * <p>On {@link #start()}, discovers the initial partitions via the Bigtable API and distributes
 * them to available readers using a least-loaded strategy. Supports cooperative rebalancing when
 * new readers join.
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

    /** Tracks how many splits each reader currently owns, for least-loaded assignment. */
    private final Map<Integer, Integer> readerSplitCounts = new HashMap<>();

    private transient BigtableDataClient client;

    // Metrics
    private final Counter partitionChangedEventsReceived;
    private final Counter splitsAssigned;
    private final Counter splitsAddedBack;
    private final Counter initialPartitionsDiscovered;
    private final Counter partitionChangedNoReaders;
    private final Counter unknownSourceEvents;
    private final Counter splitsRebalanced;
    private final Counter splitsRebalanceRequested;

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
        splitsRebalanced = context.metricGroup().counter("splits_rebalanced");
        splitsRebalanceRequested = context.metricGroup().counter("splits_rebalance_requested");
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
            readerSplitCounts.merge(subtaskId, 1, Integer::sum);
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
        // Clear the failed reader's count
        readerSplitCounts.remove(subtaskId);
        pendingSplits.addAll(splits);
        assignPendingSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("Reader {} registered", subtaskId);
        readerSplitCounts.putIfAbsent(subtaskId, 0);
        assignPendingSplits();
        triggerRebalanceIfNeeded();
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof PartitionChangedEvent) {
            PartitionChangedEvent event = (PartitionChangedEvent) sourceEvent;
            partitionChangedEventsReceived.inc();

            // Decrement sender's count — the closed partition is gone
            readerSplitCounts.merge(subtaskId, -1, Integer::sum);
            if (readerSplitCounts.getOrDefault(subtaskId, 0) < 0) {
                readerSplitCounts.put(subtaskId, 0);
            }

            List<BigtableChangeStreamSplit> newSplits = event.getNewSplits();
            LOG.info(
                    "Received PartitionChangedEvent from subtask {}: closedPartition={}, "
                            + "status={}, {} new split(s)",
                    subtaskId,
                    event.getClosedPartitionId(),
                    event.getCloseStreamStatus(),
                    newSplits.size());
            pendingSplits.addAll(newSplits);
            assignPendingSplits();
        } else if (sourceEvent instanceof SplitsReleasedEvent) {
            SplitsReleasedEvent event = (SplitsReleasedEvent) sourceEvent;
            List<BigtableChangeStreamSplit> released = event.getReleasedSplits();
            splitsRebalanced.inc(released.size());

            // Decrement sender's count for each released split
            readerSplitCounts.merge(subtaskId, -released.size(), Integer::sum);
            if (readerSplitCounts.getOrDefault(subtaskId, 0) < 0) {
                readerSplitCounts.put(subtaskId, 0);
            }

            LOG.info(
                    "Received SplitsReleasedEvent from subtask {}: {} split(s) released",
                    subtaskId,
                    released.size());
            pendingSplits.addAll(released);
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

    /** Assigns ALL pending splits to the least-loaded readers. */
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

        while (!pendingSplits.isEmpty()) {
            BigtableChangeStreamSplit split = pendingSplits.poll();
            if (split == null) {
                break;
            }
            int target = leastLoadedReader(registeredReaders);
            context.assignSplit(split, target);
            readerSplitCounts.merge(target, 1, Integer::sum);
            splitsAssigned.inc();
            LOG.info("Assigned split {} to subtask {}", split.splitId(), target);
        }
    }

    /** Returns the subtask ID with the fewest tracked splits, breaking ties by lowest ID. */
    private int leastLoadedReader(int[] registeredReaders) {
        int best = registeredReaders[0];
        int bestCount = readerSplitCounts.getOrDefault(best, 0);
        for (int i = 1; i < registeredReaders.length; i++) {
            int count = readerSplitCounts.getOrDefault(registeredReaders[i], 0);
            if (count < bestCount || (count == bestCount && registeredReaders[i] < best)) {
                best = registeredReaders[i];
                bestCount = count;
            }
        }
        return best;
    }

    /**
     * Checks whether any reader is significantly overloaded relative to the ideal distribution, and
     * sends {@link RebalanceRequestEvent}s to request voluntary split release.
     */
    private void triggerRebalanceIfNeeded() {
        int numReaders = context.registeredReaders().size();
        if (numReaders < 2) {
            return;
        }

        int totalSplits = 0;
        for (int count : readerSplitCounts.values()) {
            totalSplits += count;
        }
        int ideal = totalSplits / numReaders;

        for (Map.Entry<Integer, Integer> entry : readerSplitCounts.entrySet()) {
            int excess = entry.getValue() - (ideal + 1);
            if (excess > 0) {
                splitsRebalanceRequested.inc();
                context.sendEventToSourceReader(entry.getKey(), new RebalanceRequestEvent(excess));
                LOG.info(
                        "Sent RebalanceRequestEvent({}) to reader {} (has {}, ideal {})",
                        excess,
                        entry.getKey(),
                        entry.getValue(),
                        ideal);
            }
        }
    }
}
