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

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Reads Bigtable Change Stream partitions assigned as {@link BigtableChangeStreamSplit}s.
 *
 * <p>For each assigned split, opens a blocking {@code ReadChangeStream} call on a background thread
 * and buffers deserialized {@link RowData} records for the Flink runtime to poll.
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
    private final String messageClassName;
    private final RowType rowType;
    private final String rowKeyField;
    private final int startLookbackSeconds;

    // Row key injection: resolved from rowKeyField during start()
    private int rowKeyFieldIndex = -1;
    private LogicalTypeRoot rowKeyTypeRoot;

    private transient BigtableDataClient client;
    private transient Message defaultInstance;
    private transient Descriptor protoDescriptor;

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

    // Split management
    private final Queue<BigtableChangeStreamSplit> assignedSplits = new ArrayDeque<>();
    private volatile BigtableChangeStreamSplit currentSplit;
    private volatile Thread streamThread;
    private volatile boolean finished = false;
    private volatile Throwable streamError;
    private volatile long currentSplitStartTimeMs;

    // Bounded buffer for records produced by the stream thread.
    // The stream thread blocks on offer() when the buffer is full, providing backpressure.
    static final int RECORD_BUFFER_CAPACITY = 1000;
    private static final long BUFFER_OFFER_TIMEOUT_MS = 100;
    private final LinkedBlockingQueue<RowData> recordBuffer =
            new LinkedBlockingQueue<>(RECORD_BUFFER_CAPACITY);

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
            String messageClassName,
            RowType rowType,
            String rowKeyField,
            int startLookbackSeconds) {
        this.readerContext = readerContext;
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.tableId = tableId;
        this.columnFamily = columnFamily;
        this.cellColumn = cellColumn;
        this.messageClassName = messageClassName;
        this.rowType = rowType;
        this.rowKeyField = rowKeyField;
        this.startLookbackSeconds = startLookbackSeconds;
    }

    @Override
    public void start() {
        try {
            BigtableDataSettings.Builder builder =
                    BigtableDataSettings.newBuilder()
                            .setProjectId(projectId)
                            .setInstanceId(instanceId);

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

        initProto();
        initRowKeyField();

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
                .gauge(
                        "buffer_utilization",
                        () -> (double) recordBuffer.size() / RECORD_BUFFER_CAPACITY);

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

        LOG.info(
                "SourceReader started: project={}, instance={}, table={}",
                projectId,
                instanceId,
                tableId);
    }

    private void initProto() {
        try {
            Class<?> protoClass = Class.forName(messageClassName);
            defaultInstance = (Message) protoClass.getMethod("getDefaultInstance").invoke(null);
            protoDescriptor = defaultInstance.getDescriptorForType();
        } catch (Exception e) {
            throw new RuntimeException("Failed to load protobuf class: " + messageClassName, e);
        }
    }

    private void initRowKeyField() {
        if (rowKeyField == null || rowKeyField.isEmpty()) {
            return;
        }
        List<RowType.RowField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(rowKeyField)) {
                rowKeyFieldIndex = i;
                rowKeyTypeRoot = fields.get(i).getType().getTypeRoot();
                LOG.info(
                        "Row key field '{}' mapped to index {} (type {})",
                        rowKeyField,
                        i,
                        rowKeyTypeRoot);
                return;
            }
        }
        throw new IllegalArgumentException(
                "row-key-field '" + rowKeyField + "' not found in schema: " + rowType);
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        if (streamError != null) {
            throw new RuntimeException("Error in change stream reader thread", streamError);
        }

        // Single lock covers buffer check + future reset to prevent lost notifications.
        // Without this, notifyAvailable() could complete the old future between our buffer
        // check and the future swap, causing Flink to block on a never-completed future.
        synchronized (lock) {
            RowData row = recordBuffer.poll();
            if (row != null) {
                output.collect(row);
                return InputStatus.MORE_AVAILABLE;
            }

            // No records buffered — check if we should start reading the next split
            if (streamThread == null || !streamThread.isAlive()) {
                synchronized (assignedSplits) {
                    BigtableChangeStreamSplit next = assignedSplits.poll();
                    if (next != null) {
                        startReadingSplit(next);
                        return InputStatus.NOTHING_AVAILABLE;
                    }
                }

                if (finished) {
                    return InputStatus.END_OF_INPUT;
                }
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
        synchronized (assignedSplits) {
            assignedSplits.addAll(splits);
        }
        LOG.info("Added {} split(s) to reader", splits.size());
        notifyAvailable();
    }

    @Override
    public void notifyNoMoreSplits() {
        LOG.info("No more splits will be assigned");
        // We keep running — change streams are unbounded.
        // This just means the enumerator has assigned all initial partitions.
    }

    @Override
    public List<BigtableChangeStreamSplit> snapshotState(long checkpointId) {
        // Return current split (with latest token) plus any unstarted splits
        java.util.List<BigtableChangeStreamSplit> state = new java.util.ArrayList<>();
        if (currentSplit != null) {
            state.add(currentSplit);
        }
        synchronized (assignedSplits) {
            state.addAll(assignedSplits);
        }
        return state;
    }

    @Override
    public void close() throws Exception {
        finished = true;
        // Close client first to unblock the blocking ReadChangeStream RPC iterator,
        // then interrupt + join the thread. Thread.interrupt() alone may not unblock gRPC calls.
        if (client != null) {
            client.close();
            LOG.info("Closed reader BigtableDataClient");
        }
        if (streamThread != null) {
            streamThread.interrupt();
            streamThread.join(5000);
            if (streamThread.isAlive()) {
                LOG.warn(
                        "Stream thread {} still alive after 5s join timeout",
                        streamThread.getName());
            }
        }
    }

    private void startReadingSplit(BigtableChangeStreamSplit split) {
        currentSplit = split;
        currentSplitStartTimeMs = System.currentTimeMillis();
        streamThread =
                new Thread(
                        () -> {
                            try {
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
                            }
                        },
                        "bt-cs-" + Integer.toHexString(split.splitId().hashCode()));
        streamThread.setDaemon(true);
        streamThread.start();
        streamThreadStarted.inc();
    }

    private void readPartition(BigtableChangeStreamSplit split) {
        ReadChangeStreamQuery query =
                ReadChangeStreamQuery.create(tableId)
                        .streamPartition(split.getPartition())
                        .heartbeatDuration(org.threeten.bp.Duration.ofSeconds(30));

        if (split.getContinuationToken() != null) {
            LOG.info("Resuming partition {} from continuation token", split.splitId());
            query.continuationTokens(
                    Collections.singletonList(
                            ChangeStreamContinuationToken.create(
                                    split.getPartition(), split.getContinuationToken())));
        } else {
            org.threeten.bp.Instant startTime =
                    org.threeten.bp.Instant.now().minusSeconds(startLookbackSeconds);
            query.startTime(startTime);
            LOG.info("Starting partition {} from {}s ago", split.splitId(), startLookbackSeconds);
        }

        boolean receivedCloseStream = false;

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

                byte[] protoBytes = extractProtoBytes(mutation);
                String token = mutation.getToken();

                if (protoBytes != null) {
                    try {
                        String rowKey =
                                rowKeyFieldIndex >= 0 ? mutation.getRowKey().toStringUtf8() : null;
                        RowData row = deserialize(protoBytes, rowKey);
                        recordsDeserialized.inc();
                        // Block if buffer is full — applies backpressure to the gRPC stream
                        while (!finished) {
                            if (recordBuffer.offer(
                                    row, BUFFER_OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                                break;
                            }
                            bufferFullEvents.inc();
                        }
                        currentSplit = split.withToken(token);
                        notifyAvailable();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        LOG.error("Failed to deserialize proto: {}", e.getMessage(), e);
                        deserializationErrors.inc();
                        recordsSkipped.inc();
                        currentSplit = split.withToken(token);
                    }
                } else {
                    nullProtoBytes.inc();
                    recordsSkipped.inc();
                    currentSplit = split.withToken(token);
                }
            } else if (record instanceof Heartbeat) {
                heartbeatsReceived.inc();
                Heartbeat heartbeat = (Heartbeat) record;
                currentSplit =
                        split.withToken(heartbeat.getChangeStreamContinuationToken().getToken());
            } else if (record instanceof CloseStream) {
                receivedCloseStream = true;
                closeStreamReceived.inc();
                CloseStream closeStream = (CloseStream) record;
                List<ChangeStreamContinuationToken> newTokens =
                        closeStream.getChangeStreamContinuationTokens();
                LOG.info(
                        "CloseStream received for partition {}: status={}, newPartitions={}",
                        split.splitId(),
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
                                    newSplits,
                                    split.splitId(),
                                    closeStream.getStatus().toString()));
                    LOG.info(
                            "Sent {} new split(s) to enumerator after partition change",
                            newSplits.size());
                } else {
                    closeStreamEmptyTokens.inc();
                    LOG.warn(
                            "CloseStream for partition {} had zero continuation tokens — partition may be lost",
                            split.splitId());
                }

                long lifetime = System.currentTimeMillis() - currentSplitStartTimeMs;
                partitionLifetimeMs.update(lifetime);
                lastPartitionLifetimeMs = lifetime;

                // Current partition is done — clear it so we pick up the next assigned split
                currentSplit = null;
                break;
            }
        }

        if (!finished && !receivedCloseStream) {
            streamExhaustedWithoutCloseStream.inc();
            LOG.warn(
                    "Stream iterator for partition {} ended without CloseStream — connection may have died",
                    split.splitId());
        }
    }

    private byte[] extractProtoBytes(ChangeStreamMutation mutation) {
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

    private GenericRowData deserialize(byte[] protoBytes, String rowKey) throws Exception {
        Message message = defaultInstance.getParserForType().parseFrom(protoBytes);
        List<RowType.RowField> fields = rowType.getFields();
        GenericRowData row = new GenericRowData(fields.size());

        for (int i = 0; i < fields.size(); i++) {
            // Inject the row key for the designated field instead of reading from proto
            if (i == rowKeyFieldIndex && rowKey != null) {
                row.setField(i, parseRowKey(rowKey, rowKeyTypeRoot));
                continue;
            }

            RowType.RowField field = fields.get(i);
            FieldDescriptor fd = protoDescriptor.findFieldByName(field.getName());

            if (fd == null) {
                row.setField(i, null);
                continue;
            }

            if (fd.hasPresence() && !message.hasField(fd)) {
                row.setField(i, null);
                continue;
            }

            Object value = message.getField(fd);
            row.setField(i, convertToFlink(value, field.getType().getTypeRoot()));
        }

        return row;
    }

    /** Parses the Bigtable row key string back into the appropriate Flink type. */
    private static Object parseRowKey(String rowKey, LogicalTypeRoot typeRoot) {
        switch (typeRoot) {
            case BIGINT:
                return Long.parseLong(rowKey);
            case INTEGER:
            case SMALLINT:
            case TINYINT:
                return Integer.parseInt(rowKey);
            case VARCHAR:
            case CHAR:
                return StringData.fromString(rowKey);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported row key type for deserialization: " + typeRoot);
        }
    }

    private static Object convertToFlink(Object protoValue, LogicalTypeRoot typeRoot) {
        switch (typeRoot) {
            case BIGINT:
                return ((Number) protoValue).longValue();
            case INTEGER:
            case SMALLINT:
            case TINYINT:
                return ((Number) protoValue).intValue();
            case BOOLEAN:
                return protoValue;
            case VARCHAR:
            case CHAR:
                return StringData.fromString(protoValue.toString());
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Flink type for proto deserialization: " + typeRoot);
        }
    }

    private void notifyAvailable() {
        synchronized (lock) {
            availableFuture.complete(null);
        }
    }
}
