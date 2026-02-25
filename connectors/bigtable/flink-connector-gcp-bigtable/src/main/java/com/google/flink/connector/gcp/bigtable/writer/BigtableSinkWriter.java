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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.Counter;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.flink.connector.gcp.bigtable.serializers.BaseRowMutationSerializer;
import com.google.flink.connector.gcp.bigtable.utils.ErrorMessages;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Entry class for the writer.
 *
 * <p>This class implements the interface for the actual writer {@link BigtableFlushableWriter}.
 *
 * <p>Method "write" serializes the incoming element and collects it in a buffer. During checkpoint
 * "flush" is called to send the {@link RowMutationEntry} to Bigtable.
 */
public class BigtableSinkWriter<T> implements SinkWriter<T> {
    private final BigtableFlushableWriter writer;
    private final BaseRowMutationSerializer<T> serializer;
    private Counter numSerializationErrorsCounter;

    public BigtableSinkWriter(
            BigtableFlushableWriter writer,
            BaseRowMutationSerializer<T> serializer,
            WriterInitContext context)
            throws IOException {
        this.writer = writer;
        this.serializer = serializer;
        this.numSerializationErrorsCounter =
                context.metricGroup().counter("numSerializationErrorsCounter");
    }

    /**
     * Serializes and collects elements. Null entries (e.g. UPDATE_BEFORE in upsert mode) are
     * skipped.
     */
    @Override
    public void write(T element, Context context) throws InterruptedException {
        RowMutationEntry entry;
        try {
            entry = serializer.serialize(element, context);
        } catch (Exception e) {
            this.numSerializationErrorsCounter.inc();
            throw new RuntimeException(ErrorMessages.SERIALIZER_ERROR + e.getMessage());
        }
        if (entry != null) {
            writer.collect(entry);
        }
    }

    /** Per checkpoint, write buffered elements to Bigtable. */
    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        writer.flush();
    }

    /** Close clients and write remaining elements. */
    @Override
    public void close() throws IOException, ExecutionException, InterruptedException {
        writer.close();
    }

    @VisibleForTesting
    Counter getNumSerializationErrorsCounter() {
        return numSerializationErrorsCounter;
    }
}
