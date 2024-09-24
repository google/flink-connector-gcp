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

package com.google.flink.connector.gcp.bigtable.writer;

import org.apache.flink.api.connector.sink2.Sink;

import com.google.api.gax.batching.Batcher;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.models.TableId;

/**
 * Flushable writer that sends records to Bigtable during "flush"/
 *
 * <p>Method "collect" adds elements to the {@link Batcher}, which gets flushed during "flush".
 *
 * <p>At closing, all records are flushed and clients closed.
 */
public class BigtableFlushableWriter {
    BigtableDataClient client;
    Batcher<RowMutationEntry, Void> batcher;

    Sink.InitContext context;

    public BigtableFlushableWriter(
            BigtableDataClient client, Sink.InitContext sinkInitContext, String table) {
        this.client = client;
        this.batcher = client.newBulkMutationBatcher(TableId.of(table));
        this.context = sinkInitContext;
    }

    /** Adds RowMuationEntry to Batcher. */
    public void collect(RowMutationEntry entry) throws InterruptedException {
        batcher.add(entry);
    }

    /** Sends mutations to Bigtable. */
    public void flush() throws InterruptedException {
        batcher.flush();
    }

    /** Send outstanding mutations and close clients. */
    public void close() throws InterruptedException {
        flush();
        batcher.close();
        client.close();
    }
}
