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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.flink.shaded.guava33.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.CloseStream;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.Heartbeat;
import com.google.cloud.bigtable.data.v2.models.ReadChangeStreamQuery;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.protobuf.ByteString;
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
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
    private final String appProfileId;
    private final String columnFamily;
    private final String cellColumn;
    private final ByteString cellColumnBytes;
    private final RowKeyInjectingDeserializationSchema deserializationSchema;
    private final boolean emitDeletes;
    private final int startLookbackSeconds;
    private final int bufferCapacity;
    private final int grpcChannelPoolSize;
    private final int maxPartitionThreads;
    private final Supplier<BigtableDataClient> clientFactory;

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
    private volatile long lastNotificationLatencyMs;
    private transient Counter mutationsReceived;
    private transient Counter recordsDeserialized;
    private transient Counter recordsSkipped;

    // CloseStream lifecycle metrics
    private transient Counter closeStreamReceived;
    private transient Counter closeStreamEmptyTokens;
    private transient Counter partitionSplitsCreated;
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

    // Delete emission metrics
    private transient Counter deleteRecordsEmitted;

    // Rebalancing metrics
    private transient Counter splitsRebalanced;

    private volatile boolean finished = false;
    private volatile Throwable streamError;

    // gRPC timeout overrides — ReadChangeStream is a long-lived streaming RPC that can run
    // for hours/days. The default attempt/wait timeouts cause DEADLINE_EXCEEDED on idle streams.
    private static final java.time.Duration STREAM_IDLE_TIMEOUT = java.time.Duration.ofHours(1);
    private static final java.time.Duration STREAM_WAIT_TIMEOUT = java.time.Duration.ofMinutes(30);
    private static final java.time.Duration STREAM_TOTAL_TIMEOUT = java.time.Duration.ofDays(7);
    private static final java.time.Duration STREAM_RPC_TIMEOUT = java.time.Duration.ofHours(6);

    // Heartbeat interval for the ReadChangeStream query — Bigtable sends a Heartbeat record
    // at this interval when there are no mutations, keeping the stream alive.
    private static final java.time.Duration HEARTBEAT_DURATION = java.time.Duration.ofSeconds(30);

    // Default maximum number of concurrent partition reader threads.
    static final int DEFAULT_MAX_PARTITION_THREADS = 64;

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
            String appProfileId,
            String columnFamily,
            String cellColumn,
            RowKeyInjectingDeserializationSchema deserializationSchema,
            boolean emitDeletes,
            int startLookbackSeconds,
            int bufferCapacity,
            int grpcChannelPoolSize,
            int maxPartitionThreads) {
        this(
                readerContext,
                projectId,
                instanceId,
                tableId,
                appProfileId,
                columnFamily,
                cellColumn,
                deserializationSchema,
                emitDeletes,
                startLookbackSeconds,
                bufferCapacity,
                grpcChannelPoolSize,
                maxPartitionThreads,
                null);
    }

    /**
     * Constructor that accepts a {@link BigtableDataClient} supplier for testability.
     *
     * <p>When {@code clientFactory} is non-null, {@link #start()} uses it instead of creating a
     * client from the project/instance settings.
     */
    @VisibleForTesting
    BigtableChangeStreamSourceReader(
            SourceReaderContext readerContext,
            String projectId,
            String instanceId,
            String tableId,
            String appProfileId,
            String columnFamily,
            String cellColumn,
            RowKeyInjectingDeserializationSchema deserializationSchema,
            boolean emitDeletes,
            int startLookbackSeconds,
            int bufferCapacity,
            int grpcChannelPoolSize,
            int maxPartitionThreads,
            Supplier<BigtableDataClient> clientFactory) {
        this.readerContext = readerContext;
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.tableId = tableId;
        this.appProfileId = appProfileId;
        this.columnFamily = columnFamily;
        this.cellColumn = cellColumn;
        this.cellColumnBytes = ByteString.copyFromUtf8(cellColumn);
        this.deserializationSchema = deserializationSchema;
        this.emitDeletes = emitDeletes;
        this.startLookbackSeconds = startLookbackSeconds;
        this.bufferCapacity = bufferCapacity > 0 ? bufferCapacity : DEFAULT_RECORD_BUFFER_CAPACITY;
        this.grpcChannelPoolSize = grpcChannelPoolSize;
        this.maxPartitionThreads =
                maxPartitionThreads > 0 ? maxPartitionThreads : DEFAULT_MAX_PARTITION_THREADS;
        this.clientFactory = clientFactory;
        this.recordBuffer = new LinkedBlockingQueue<>(this.bufferCapacity);
    }

    @Override
    public void start() {
        if (clientFactory != null) {
            client = clientFactory.get();
        } else {
            try {
                BigtableDataSettings.Builder builder =
                        BigtableDataSettings.newBuilder()
                                .setProjectId(projectId)
                                .setInstanceId(instanceId);

                if (appProfileId != null && !appProfileId.isEmpty()) {
                    builder.setAppProfileId(appProfileId);
                }

                if (grpcChannelPoolSize > 0) {
                    builder.stubSettings()
                            .setTransportChannelProvider(
                                    com.google.api.gax.grpc.InstantiatingGrpcChannelProvider
                                            .newBuilder()
                                            .setPoolSize(grpcChannelPoolSize)
                                            .build());
                }

                builder.stubSettings()
                        .readChangeStreamSettings()
                        .setIdleTimeoutDuration(STREAM_IDLE_TIMEOUT)
                        .setWaitTimeoutDuration(STREAM_WAIT_TIMEOUT)
                        .setRetrySettings(
                                builder
                                        .stubSettings()
                                        .readChangeStreamSettings()
                                        .getRetrySettings()
                                        .toBuilder()
                                        .setTotalTimeoutDuration(STREAM_TOTAL_TIMEOUT)
                                        .setInitialRpcTimeoutDuration(STREAM_RPC_TIMEOUT)
                                        .setMaxRpcTimeoutDuration(STREAM_RPC_TIMEOUT)
                                        .build());

                client = BigtableDataClient.create(builder.build());
            } catch (IOException e) {
                throw new RuntimeException("Failed to create BigtableDataClient in reader", e);
            }
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

        // SynchronousQueue is intentional here: with corePoolSize=0, ThreadPoolExecutor
        // only creates new threads when the queue rejects a task. An unbounded queue (e.g.
        // LinkedBlockingQueue) would never reject, so no threads would ever be created.
        // SynchronousQueue forces immediate hand-off, creating threads on demand up to
        // maxPartitionThreads. Idle threads are reclaimed after 60s keep-alive.
        // If all threads are busy, submit() throws RejectedExecutionException — this
        // indicates maxPartitionThreads is undersized for the partition count.
        executor =
                new java.util.concurrent.ThreadPoolExecutor(
                        0,
                        maxPartitionThreads,
                        60L,
                        TimeUnit.SECONDS,
                        new java.util.concurrent.SynchronousQueue<>(),
                        new ThreadFactoryBuilder()
                                .setNameFormat(
                                        "bigtable-cs-reader-"
                                                + readerContext.getIndexOfSubtask()
                                                + "-%d")
                                .setDaemon(true)
                                .build());

        // Register Flink metrics
        readerContext
                .metricGroup()
                .gauge(
                        "bigtable_changestream_notification_latency_ms",
                        () -> lastNotificationLatencyMs);
        mutationsReceived =
                readerContext.metricGroup().counter("bigtable_changestream_mutations_received");
        recordsDeserialized =
                readerContext.metricGroup().counter("bigtable_changestream_records_deserialized");
        recordsSkipped =
                readerContext.metricGroup().counter("bigtable_changestream_records_skipped");

        // CloseStream lifecycle
        closeStreamReceived =
                readerContext.metricGroup().counter("bigtable_changestream_closestream_received");
        closeStreamEmptyTokens =
                readerContext
                        .metricGroup()
                        .counter("bigtable_changestream_closestream_empty_tokens");
        partitionSplitsCreated =
                readerContext
                        .metricGroup()
                        .counter("bigtable_changestream_partition_splits_created");
        readerContext
                .metricGroup()
                .gauge(
                        "bigtable_changestream_partition_lifetime_ms",
                        () -> lastPartitionLifetimeMs);

        // Buffer backpressure
        bufferFullEvents =
                readerContext.metricGroup().counter("bigtable_changestream_buffer_full_events");
        readerContext
                .metricGroup()
                .gauge(
                        "bigtable_changestream_buffer_utilization",
                        () -> (double) recordBuffer.size() / bufferCapacity);

        // Stream thread lifecycle
        streamThreadStarted =
                readerContext.metricGroup().counter("bigtable_changestream_stream_thread_started");
        streamThreadErrors =
                readerContext.metricGroup().counter("bigtable_changestream_stream_thread_errors");
        streamThreadCompleted =
                readerContext
                        .metricGroup()
                        .counter("bigtable_changestream_stream_thread_completed");

        // Error categorization
        deserializationErrors =
                readerContext.metricGroup().counter("bigtable_changestream_deserialization_errors");
        nullProtoBytes =
                readerContext.metricGroup().counter("bigtable_changestream_null_proto_bytes");
        deleteRecordsEmitted =
                readerContext.metricGroup().counter("bigtable_changestream_delete_records_emitted");

        // gRPC stream lifecycle
        streamExhaustedWithoutCloseStream =
                readerContext
                        .metricGroup()
                        .counter("bigtable_changestream_stream_exhausted_without_closestream");
        heartbeatsReceived =
                readerContext.metricGroup().counter("bigtable_changestream_heartbeats_received");

        // Rebalancing
        splitsRebalanced =
                readerContext.metricGroup().counter("bigtable_changestream_splits_rebalanced");

        // Active partitions gauge
        readerContext
                .metricGroup()
                .gauge("bigtable_changestream_active_partitions", () -> activeSplits.size());

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

        // Lock only covers buffer check + future reset to prevent lost notifications.
        // output.collect() is outside the lock to avoid contention with producer threads.
        RowData row;
        synchronized (lock) {
            row = recordBuffer.poll();
            if (row == null) {
                if (finished && activeSplits.isEmpty()) {
                    return InputStatus.END_OF_INPUT;
                }

                // Reset the availability future so Flink waits for the next notification
                if (availableFuture.isDone()) {
                    availableFuture = new CompletableFuture<>();
                }
                return InputStatus.NOTHING_AVAILABLE;
            }
        }

        output.collect(row);
        return InputStatus.MORE_AVAILABLE;
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
                LOG.info("Released {} split(s) for rebalancing", released.size());
                // Notify the enumerator so it can re-assign these splits to other readers.
                // The finally block in startReadingSplit() will NOT double-send because
                // activeSplits.remove() above already claimed the split atomically —
                // the finally block's remove() returns null, so its guard
                // (!closedCleanly && latestSplit != null) prevents a duplicate event.
                readerContext.sendSourceEventToCoordinator(new SplitsReleasedEvent(released));
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

        Future<?> future;
        try {
            future =
                    executor.submit(
                            () -> {
                                boolean closedCleanly = false;
                                try {
                                    streamThreadStarted.inc();
                                    closedCleanly = readPartition(split);
                                    streamThreadCompleted.inc();
                                } catch (Exception e) {
                                    if (!finished) {
                                        if (!(e instanceof InterruptedException)) {
                                            LOG.error(
                                                    "Error reading partition {}: {}",
                                                    split.splitId(),
                                                    e.getMessage(),
                                                    e);
                                            streamThreadErrors.inc();
                                            streamError = e;
                                            notifyAvailable();
                                        }
                                    }
                                } finally {
                                    BigtableChangeStreamSplit latestSplit =
                                            activeSplits.remove(splitId);
                                    splitStartTimes.remove(splitId);
                                    activeThreads.remove(splitId);
                                    if (!closedCleanly && latestSplit != null) {
                                        readerContext.sendSourceEventToCoordinator(
                                                new SplitsReleasedEvent(
                                                        Collections.singletonList(latestSplit)));
                                    }
                                }
                            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            activeSplits.remove(splitId);
            splitStartTimes.remove(splitId);
            throw new RuntimeException(
                    String.format(
                            "Cannot start reading partition %s: all %d partition reader threads "
                                    + "are in use. Increase 'max-partition-threads' (current: %d) "
                                    + "to match the number of Bigtable partitions assigned to "
                                    + "this reader.",
                            splitId, maxPartitionThreads, maxPartitionThreads),
                    e);
        }
        activeThreads.put(splitId, future);
    }

    private boolean readPartition(BigtableChangeStreamSplit split) {
        String splitId = split.splitId();
        ReadChangeStreamQuery query =
                ReadChangeStreamQuery.create(tableId)
                        .streamPartition(split.getPartition())
                        .heartbeatDuration(HEARTBEAT_DURATION);

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
                lastNotificationLatencyMs = latency;
                mutationsReceived.inc();

                byte[] cellBytes = extractCellBytes(mutation);
                String token = mutation.getToken();

                if (cellBytes != null) {
                    try {
                        byte[] rowKeyBytes = mutation.getRowKey().toByteArray();
                        RowData row =
                                deserializationSchema.deserializeWithRowKey(cellBytes, rowKeyBytes);
                        if (row == null) {
                            recordsSkipped.inc();
                            activeSplits.put(splitId, split.withToken(token));
                            continue;
                        }
                        recordsDeserialized.inc();
                        // Block if buffer is full — applies backpressure to the gRPC stream.
                        // Count once per record that encounters a full buffer, not per retry.
                        boolean counted = false;
                        while (!finished) {
                            if (recordBuffer.offer(
                                    row, BUFFER_OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                                break;
                            }
                            if (!counted) {
                                bufferFullEvents.inc();
                                counted = true;
                            }
                        }
                        activeSplits.put(splitId, split.withToken(token));
                        notifyAvailable();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        // Skip malformed records and track via metrics. This avoids failing the
                        // entire job on a single bad record while providing visibility through
                        // deserializationErrors and recordsSkipped counters.
                        // TODO: Make this configurable (e.g. fail-on-deserialization-error) for
                        //  use cases where data integrity requires failing fast.
                        LOG.error("Failed to deserialize record: {}", e.getMessage(), e);
                        deserializationErrors.inc();
                        recordsSkipped.inc();
                        activeSplits.put(splitId, split.withToken(token));
                    }
                } else {
                    // No matching SetCell entry. Check for delete entries to emit as
                    // RowKind.DELETE.
                    //
                    // Mutation-type-to-RowKind mapping:
                    //   SetCell        → RowKind.INSERT (handled above)
                    //   DeleteCells    → RowKind.DELETE (targeted cell/qualifier delete)
                    //   DeleteFamily   → RowKind.DELETE (entire column family delete)
                    //
                    // Both delete types are scoped to a single row key within a single
                    // ChangeStreamMutation — a DeleteFamily does NOT fan out into multiple
                    // rows. Application-level batch deletes (e.g. deleting 1000 rows)
                    // produce 1000 separate mutations, each emitting one DELETE row.
                    //
                    // Mixed mutations (SetCell + delete entries): the SetCell path above
                    // already emitted an INSERT, so we only reach here when no matching
                    // SetCell was found. This handles the "pure delete" case.
                    //
                    // Requires row-key-field to be configured — without it, there is no key
                    // to identify what was deleted, so the mutation is skipped.
                    if (emitDeletes
                            && deserializationSchema.hasRowKeyField()
                            && hasDeleteEntries(mutation)) {
                        try {
                            byte[] rowKeyBytes = mutation.getRowKey().toByteArray();
                            RowData deleteRow = deserializationSchema.createDeleteRow(rowKeyBytes);
                            if (deleteRow != null) {
                                deleteRecordsEmitted.inc();
                                boolean counted = false;
                                while (!finished) {
                                    if (recordBuffer.offer(
                                            deleteRow,
                                            BUFFER_OFFER_TIMEOUT_MS,
                                            TimeUnit.MILLISECONDS)) {
                                        break;
                                    }
                                    if (!counted) {
                                        bufferFullEvents.inc();
                                        counted = true;
                                    }
                                }
                                activeSplits.put(splitId, split.withToken(token));
                                notifyAvailable();
                            } else {
                                nullProtoBytes.inc();
                                recordsSkipped.inc();
                                activeSplits.put(splitId, split.withToken(token));
                            }
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    } else {
                        nullProtoBytes.inc();
                        recordsSkipped.inc();
                        activeSplits.put(splitId, split.withToken(token));
                    }
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
                    // Re-enqueue with the latest split (may have updated continuation
                    // token from heartbeats/mutations) so recovery doesn't reprocess data
                    BigtableChangeStreamSplit latestSplit = activeSplits.get(splitId);
                    readerContext.sendSourceEventToCoordinator(
                            new PartitionChangedEvent(
                                    Collections.singletonList(
                                            latestSplit != null ? latestSplit : split),
                                    splitId,
                                    closeStream.getStatus().toString()));
                }

                long lifetime = System.currentTimeMillis() - partitionStartTimeMs;
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
        return receivedCloseStream;
    }

    /**
     * Extracts the cell value bytes from the configured column family and column qualifier.
     *
     * <p>Only {@link SetCell} entries are processed. Delete entries ({@code DeleteCells}, {@code
     * DeleteFamily}) are intentionally skipped — delete operations do not carry a cell value
     * payload. Mutations containing only delete entries will return {@code null} and be counted as
     * skipped records.
     *
     * <p>Returns the <b>first</b> matching {@code SetCell} entry. If a mutation contains multiple
     * entries for the same column family and qualifier, only the first is returned.
     *
     * <p><b>Note:</b> The mutation's {@code tieBreaker} field (used to order mutations with the
     * same commit timestamp) is not exposed. It could be added as a metadata field in a future
     * version.
     *
     * @return the cell bytes, or {@code null} if no matching cell was found
     */
    @VisibleForTesting
    byte[] extractCellBytes(ChangeStreamMutation mutation) {
        for (Entry entry : mutation.getEntries()) {
            // Only SetCell entries carry a value payload; DeleteCells/DeleteFamily are skipped.
            if (entry instanceof SetCell) {
                SetCell setCell = (SetCell) entry;
                if (setCell.getFamilyName().equals(columnFamily)
                        && setCell.getQualifier().equals(cellColumnBytes)) {
                    return setCell.getValue().toByteArray();
                }
            }
        }
        return null;
    }

    /**
     * Returns {@code true} if the mutation contains at least one delete entry ({@link DeleteCells}
     * or {@link DeleteFamily}).
     */
    @VisibleForTesting
    boolean hasDeleteEntries(ChangeStreamMutation mutation) {
        for (Entry entry : mutation.getEntries()) {
            if (entry instanceof DeleteCells || entry instanceof DeleteFamily) {
                return true;
            }
        }
        return false;
    }

    private void notifyAvailable() {
        synchronized (lock) {
            availableFuture.complete(null);
        }
    }
}
