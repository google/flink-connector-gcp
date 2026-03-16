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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.UserCodeClassLoader;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.CloseStream;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.Heartbeat;
import com.google.cloud.bigtable.data.v2.models.ReadChangeStreamQuery;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Reads Bigtable Change Stream partitions assigned as {@link BigtableChangeStreamSplit}s.
 *
 * <p>For each assigned split, opens a blocking {@code ReadChangeStream} call on a dedicated thread
 * (one thread per partition via a cached thread pool) and buffers deserialized {@link RowData}
 * records for the Flink runtime to poll.
 *
 * <p>Supports cooperative rebalancing via {@link RebalanceRequestEvent} and {@link
 * SplitsReleasedEvent}.
 */
public class BigtableChangeStreamSourceReader
        implements SourceReader<RowData, BigtableChangeStreamSplit> {

    private static final Logger LOG =
            LoggerFactory.getLogger(BigtableChangeStreamSourceReader.class);

    private final SourceReaderContext readerContext;
    private final String projectId;
    private final String instanceId;
    private final String tableId;
    private final String columnFamily;
    private final String cellColumn;
    private final RowKeyInjectingDeserializationSchema deserializationSchema;
    private final int startLookbackSeconds;
    private final int bufferCapacity;
    private final int grpcChannelPoolSize;

    private transient BigtableDataClient client;

    // Concurrent partition reading: one thread per partition
    private final ConcurrentHashMap<String, BigtableChangeStreamSplit> activeSplits =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> splitStartTimes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Future<?>> activeThreads = new ConcurrentHashMap<>();
    private ExecutorService executor;

    // Splits received before start() — restored from checkpoint
    private final List<BigtableChangeStreamSplit> pendingSplitsBeforeStart = new ArrayList<>();

    // Metrics
    private transient Histogram notificationLatencyMs;
    private volatile long lastNotificationLatencyMs;
    private transient Counter mutationsReceived;
    private transient Counter recordsDeserialized;
    private transient Counter recordsSkipped;

    // CloseStream lifecycle metrics
    private transient Counter closeStreamReceived;
    private transient Counter closeStreamEmptyTokens;
    private transient Counter partitionSplitsCreated;
    private transient Histogram partitionLifetimeMs;
    private volatile long lastPartitionLifetimeMs;

    // Buffer backpressure metrics
    private transient Counter bufferFullEvents;

    // Stream thread lifecycle metrics
    private transient Counter streamThreadStarted;
    private transient Counter streamThreadErrors;
    private transient Counter streamThreadCompleted;

    // Error categorization metrics
    private transient Counter deserializationErrors;
    private transient Counter nullProtoBytes;

    // gRPC stream lifecycle metrics
    private transient Counter streamExhaustedWithoutCloseStream;
    private transient Counter heartbeatsReceived;

    // Rebalancing metrics
    private transient Counter splitsRebalanced;

    private volatile boolean finished = false;
    private volatile Throwable streamError;

    // Bounded buffer for records produced by stream threads.
    // Stream threads block on offer() when the buffer is full, providing backpressure.
    static final int DEFAULT_RECORD_BUFFER_CAPACITY = 1000;
    private static final long BUFFER_OFFER_TIMEOUT_MS = 100;
    private final LinkedBlockingQueue<RowData> recordBuffer;

    // Guards availableFuture and recordBuffer together to prevent lost-notification races.
    private final Object lock = new Object();
    private CompletableFuture<Void> availableFuture = new CompletableFuture<>();

    public BigtableChangeStreamSourceReader(
            SourceReaderContext readerContext,
            String projectId,
            String instanceId,
            String tableId,
            String columnFamily,
            String cellColumn,
            RowKeyInjectingDeserializationSchema deserializationSchema,
            int startLookbackSeconds,
            int bufferCapacity,
            int grpcChannelPoolSize) {
        this.readerContext = readerContext;
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.tableId = tableId;
        this.columnFamily = columnFamily;
        this.cellColumn = cellColumn;
        this.deserializationSchema = deserializationSchema;
        this.startLookbackSeconds = startLookbackSeconds;
        this.bufferCapacity = bufferCapacity > 0 ? bufferCapacity : DEFAULT_RECORD_BUFFER_CAPACITY;
        this.grpcChannelPoolSize = grpcChannelPoolSize;
        this.recordBuffer = new LinkedBlockingQueue<>(this.bufferCapacity);
    }

    @Override
    public void start() {
        try {
            BigtableDataSettings.Builder builder =
                    BigtableDataSettings.newBuilder()
                            .setProjectId(projectId)
                            .setInstanceId(instanceId);

            if (grpcChannelPoolSize > 0) {
                builder.stubSettings()
                        .setTransportChannelProvider(
                                com.google.api.gax.grpc.InstantiatingGrpcChannelProvider
                                        .newBuilder()
                                        .setPoolSize(grpcChannelPoolSize)
                                        .build());
            }

            // ReadChangeStream is a long-lived streaming RPC that can run for hours/days.
            // The default attempt timeout (5 min) and wait timeout (1 min) cause
            // DEADLINE_EXCEEDED when no mutations arrive within that window.
            // Override with timeouts appropriate for long-lived change streams.
            builder.stubSettings()
                    .readChangeStreamSettings()
                    .setIdleTimeoutDuration(java.time.Duration.ofHours(1))
                    .setWaitTimeoutDuration(java.time.Duration.ofMinutes(30))
                    .setRetrySettings(
                            builder
                                    .stubSettings()
                                    .readChangeStreamSettings()
                                    .getRetrySettings()
                                    .toBuilder()
                                    .setTotalTimeoutDuration(java.time.Duration.ofDays(7))
                                    .setInitialRpcTimeoutDuration(java.time.Duration.ofHours(6))
                                    .setMaxRpcTimeoutDuration(java.time.Duration.ofHours(6))
                                    .build());

            client = BigtableDataClient.create(builder.build());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create BigtableDataClient in reader", e);
        }

        // Open the pluggable format's deserialization schema
        try {
            deserializationSchema.open(
                    new DeserializationSchema.InitializationContext() {
                        @Override
                        public MetricGroup getMetricGroup() {
                            return readerContext.metricGroup();
                        }

                        @Override
                        public UserCodeClassLoader getUserCodeClassLoader() {
                            return readerContext.getUserCodeClassLoader();
                        }
                    });
        } catch (Exception e) {
            throw new RuntimeException("Failed to open deserialization schema", e);
        }

        executor =
                Executors.newCachedThreadPool(
                        r -> {
                            Thread t = new Thread(r);
                            t.setDaemon(true);
                            return t;
                        });

        // Register Flink metrics — histogram for Flink UI, gauge for Prometheus
        notificationLatencyMs =
                readerContext
                        .metricGroup()
                        .histogram(
                                "changestream_notification_latency_ms",
                                new DropwizardHistogramWrapper(
                                        new com.codahale.metrics.Histogram(
                                                new com.codahale.metrics.SlidingWindowReservoir(
                                                        1000))));
        readerContext
                .metricGroup()
                .gauge(
                        "changestream_notification_latency_ms_latest",
                        () -> lastNotificationLatencyMs);
        mutationsReceived = readerContext.metricGroup().counter("mutations_received");
        recordsDeserialized = readerContext.metricGroup().counter("records_deserialized");
        recordsSkipped = readerContext.metricGroup().counter("records_skipped");

        // CloseStream lifecycle
        closeStreamReceived = readerContext.metricGroup().counter("closestream_received");
        closeStreamEmptyTokens = readerContext.metricGroup().counter("closestream_empty_tokens");
        partitionSplitsCreated = readerContext.metricGroup().counter("partition_splits_created");
        partitionLifetimeMs =
                readerContext
                        .metricGroup()
                        .histogram(
                                "partition_lifetime_ms",
                                new DropwizardHistogramWrapper(
                                        new com.codahale.metrics.Histogram(
                                                new com.codahale.metrics.SlidingWindowReservoir(
                                                        1000))));
        readerContext
                .metricGroup()
                .gauge("partition_lifetime_ms_latest", () -> lastPartitionLifetimeMs);

        // Buffer backpressure
        bufferFullEvents = readerContext.metricGroup().counter("buffer_full_events");
        readerContext
                .metricGroup()
                .gauge("buffer_utilization", () -> (double) recordBuffer.size() / bufferCapacity);

        // Stream thread lifecycle
        streamThreadStarted = readerContext.metricGroup().counter("stream_thread_started");
        streamThreadErrors = readerContext.metricGroup().counter("stream_thread_errors");
        streamThreadCompleted = readerContext.metricGroup().counter("stream_thread_completed");

        // Error categorization
        deserializationErrors = readerContext.metricGroup().counter("deserialization_errors");
        nullProtoBytes = readerContext.metricGroup().counter("null_proto_bytes");

        // gRPC stream lifecycle
        streamExhaustedWithoutCloseStream =
                readerContext.metricGroup().counter("stream_exhausted_without_closestream");
        heartbeatsReceived = readerContext.metricGroup().counter("heartbeats_received");

        // Rebalancing
        splitsRebalanced = readerContext.metricGroup().counter("splits_rebalanced");

        // Active partitions gauge
        readerContext.metricGroup().gauge("active_partitions", () -> activeSplits.size());

        LOG.info(
                "SourceReader started: project={}, instance={}, table={}, bufferCapacity={}, "
                        + "grpcChannelPoolSize={}",
                projectId,
                instanceId,
                tableId,
                bufferCapacity,
                grpcChannelPoolSize);

        // Start any splits received before start() (restored from checkpoint)
        for (BigtableChangeStreamSplit split : pendingSplitsBeforeStart) {
            startReadingSplit(split);
        }
        pendingSplitsBeforeStart.clear();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        if (streamError != null) {
            throw new RuntimeException("Error in change stream reader thread", streamError);
        }

        // Single lock covers buffer check + future reset to prevent lost notifications.
        synchronized (lock) {
            RowData row = recordBuffer.poll();
            if (row != null) {
                output.collect(row);
                return InputStatus.MORE_AVAILABLE;
            }

            if (finished && activeSplits.isEmpty()) {
                return InputStatus.END_OF_INPUT;
            }

            // Reset the availability future so Flink waits for the next notification
            if (availableFuture.isDone()) {
                availableFuture = new CompletableFuture<>();
            }
        }

        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        synchronized (lock) {
            return availableFuture;
        }
    }

    @Override
    public void addSplits(List<BigtableChangeStreamSplit> splits) {
        if (executor == null) {
            // start() not called yet — queue for later
            pendingSplitsBeforeStart.addAll(splits);
            LOG.info("Queued {} split(s) before start()", splits.size());
            return;
        }
        for (BigtableChangeStreamSplit split : splits) {
            startReadingSplit(split);
        }
        LOG.info("Added {} split(s) to reader", splits.size());
        notifyAvailable();
    }

    @Override
    public void notifyNoMoreSplits() {
        LOG.info("No more splits will be assigned");
        // We keep running — change streams are unbounded.
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof RebalanceRequestEvent) {
            RebalanceRequestEvent request = (RebalanceRequestEvent) sourceEvent;
            int toRelease = request.getSplitsToRelease();
            LOG.info("Received rebalance request to release {} split(s)", toRelease);

            // Pick the N oldest splits by start time
            List<Map.Entry<String, Long>> sorted = new ArrayList<>(splitStartTimes.entrySet());
            sorted.sort(Map.Entry.comparingByValue());

            List<BigtableChangeStreamSplit> released = new ArrayList<>();
            for (int i = 0; i < toRelease && i < sorted.size(); i++) {
                String splitId = sorted.get(i).getKey();
                BigtableChangeStreamSplit split = activeSplits.get(splitId);
                if (split != null) {
                    // Cancel the thread and remove tracking
                    Future<?> future = activeThreads.remove(splitId);
                    if (future != null) {
                        future.cancel(true);
                    }
                    activeSplits.remove(splitId);
                    splitStartTimes.remove(splitId);
                    released.add(split);
                    splitsRebalanced.inc();
                }
            }

            if (!released.isEmpty()) {
                readerContext.sendSourceEventToCoordinator(new SplitsReleasedEvent(released));
                LOG.info("Released {} split(s) for rebalancing", released.size());
            }
        }
    }

    @Override
    public List<BigtableChangeStreamSplit> snapshotState(long checkpointId) {
        List<BigtableChangeStreamSplit> state = new ArrayList<>(activeSplits.values());
        state.addAll(pendingSplitsBeforeStart);
        return state;
    }

    @Override
    public void close() throws Exception {
        finished = true;
        // Close client first to unblock blocking ReadChangeStream RPC iterators
        if (client != null) {
            client.close();
            LOG.info("Closed reader BigtableDataClient");
        }
        if (executor != null) {
            executor.shutdownNow();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("Executor did not terminate within 5s");
            }
        }
    }

    private void startReadingSplit(BigtableChangeStreamSplit split) {
        String splitId = split.splitId();
        activeSplits.put(splitId, split);
        splitStartTimes.put(splitId, System.currentTimeMillis());

        Future<?> future =
                executor.submit(
                        () -> {
                            try {
                                streamThreadStarted.inc();
                                readPartition(split);
                                streamThreadCompleted.inc();
                            } catch (Exception e) {
                                if (!finished) {
                                    LOG.error(
                                            "Error reading partition {}: {}",
                                            split.splitId(),
                                            e.getMessage(),
                                            e);
                                    streamThreadErrors.inc();
                                    streamError = e;
                                    notifyAvailable();
                                }
                            } finally {
                                activeSplits.remove(splitId);
                                splitStartTimes.remove(splitId);
                                activeThreads.remove(splitId);
                                // Notify enumerator that this reader finished a split
                                readerContext.sendSourceEventToCoordinator(
                                        new SplitsReleasedEvent(Collections.singletonList(split)));
                            }
                        });
        activeThreads.put(splitId, future);
    }

    private void readPartition(BigtableChangeStreamSplit split) {
        String splitId = split.splitId();
        ReadChangeStreamQuery query =
                ReadChangeStreamQuery.create(tableId)
                        .streamPartition(split.getPartition())
                        .heartbeatDuration(org.threeten.bp.Duration.ofSeconds(30));

        if (split.getContinuationToken() != null) {
            LOG.info("Resuming partition {} from continuation token", splitId);
            query.continuationTokens(
                    Collections.singletonList(
                            ChangeStreamContinuationToken.create(
                                    split.getPartition(), split.getContinuationToken())));
        } else {
            org.threeten.bp.Instant startTime =
                    org.threeten.bp.Instant.now().minusSeconds(startLookbackSeconds);
            query.startTime(startTime);
            LOG.info("Starting partition {} from {}s ago", splitId, startLookbackSeconds);
        }

        boolean receivedCloseStream = false;
        long partitionStartTimeMs = System.currentTimeMillis();

        for (ChangeStreamRecord record : client.readChangeStream(query)) {
            if (finished) {
                break;
            }

            if (record instanceof ChangeStreamMutation) {
                ChangeStreamMutation mutation = (ChangeStreamMutation) record;
                long latency =
                        Math.max(
                                0,
                                System.currentTimeMillis()
                                        - mutation.getCommitTimestamp().toEpochMilli());
                notificationLatencyMs.update(latency);
                lastNotificationLatencyMs = latency;
                mutationsReceived.inc();

                byte[] cellBytes = extractCellBytes(mutation);
                String token = mutation.getToken();

                if (cellBytes != null) {
                    try {
                        String rowKey = mutation.getRowKey().toStringUtf8();
                        RowData row =
                                deserializationSchema.deserializeWithRowKey(cellBytes, rowKey);
                        recordsDeserialized.inc();
                        // Block if buffer is full — applies backpressure to the gRPC stream
                        while (!finished) {
                            if (recordBuffer.offer(
                                    row, BUFFER_OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                                break;
                            }
                            bufferFullEvents.inc();
                        }
                        activeSplits.put(splitId, split.withToken(token));
                        notifyAvailable();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        LOG.error("Failed to deserialize record: {}", e.getMessage(), e);
                        deserializationErrors.inc();
                        recordsSkipped.inc();
                        activeSplits.put(splitId, split.withToken(token));
                    }
                } else {
                    nullProtoBytes.inc();
                    recordsSkipped.inc();
                    activeSplits.put(splitId, split.withToken(token));
                }
            } else if (record instanceof Heartbeat) {
                heartbeatsReceived.inc();
                Heartbeat heartbeat = (Heartbeat) record;
                activeSplits.put(
                        splitId,
                        split.withToken(heartbeat.getChangeStreamContinuationToken().getToken()));
            } else if (record instanceof CloseStream) {
                receivedCloseStream = true;
                closeStreamReceived.inc();
                CloseStream closeStream = (CloseStream) record;
                List<ChangeStreamContinuationToken> newTokens =
                        closeStream.getChangeStreamContinuationTokens();
                LOG.info(
                        "CloseStream received for partition {}: status={}, newPartitions={}",
                        splitId,
                        closeStream.getStatus(),
                        newTokens.size());

                if (!newTokens.isEmpty()) {
                    List<BigtableChangeStreamSplit> newSplits = new ArrayList<>(newTokens.size());
                    for (ChangeStreamContinuationToken token : newTokens) {
                        newSplits.add(
                                new BigtableChangeStreamSplit(
                                        token.getPartition(), token.getToken()));
                    }
                    partitionSplitsCreated.inc(newSplits.size());
                    readerContext.sendSourceEventToCoordinator(
                            new PartitionChangedEvent(
                                    newSplits, splitId, closeStream.getStatus().toString()));
                    LOG.info(
                            "Sent {} new split(s) to enumerator after partition change",
                            newSplits.size());
                } else {
                    closeStreamEmptyTokens.inc();
                    LOG.warn(
                            "CloseStream for partition {} had zero continuation tokens "
                                    + "— re-enqueueing original split",
                            splitId);
                    // Re-enqueue the original split so the partition is not lost
                    readerContext.sendSourceEventToCoordinator(
                            new PartitionChangedEvent(
                                    Collections.singletonList(split),
                                    splitId,
                                    closeStream.getStatus().toString()));
                }

                long lifetime = System.currentTimeMillis() - partitionStartTimeMs;
                partitionLifetimeMs.update(lifetime);
                lastPartitionLifetimeMs = lifetime;
                break;
            }
        }

        if (!finished && !receivedCloseStream) {
            streamExhaustedWithoutCloseStream.inc();
            LOG.warn(
                    "Stream iterator for partition {} ended without CloseStream "
                            + "— connection may have died",
                    splitId);
        }
    }

    /**
     * Extracts the cell value bytes from the configured column family and column qualifier.
     *
     * @return the cell bytes, or {@code null} if no matching cell was found
     */
    private byte[] extractCellBytes(ChangeStreamMutation mutation) {
        for (Entry entry : mutation.getEntries()) {
            if (entry instanceof SetCell) {
                SetCell setCell = (SetCell) entry;
                if (setCell.getFamilyName().equals(columnFamily)
                        && setCell.getQualifier().toStringUtf8().equals(cellColumn)) {
                    return setCell.getValue().toByteArray();
                }
            }
        }
        return null;
    }

    private void notifyAvailable() {
        synchronized (lock) {
            availableFuture.complete(null);
        }
    }
}
