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

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.flink.connector.gcp.bigtable.serializers.BaseSerializer;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Base writer class.
 *
 * <p>This class implements the interface for the actual writer {@link BigtableFlushableWriter}.
 *
 * <p>Method "write" serializes the incoming element and collects it in a buffer. During checkpoint
 * "flush" is called to send the {@link RowMutationEntry} to Bigtable.
 */
public class BigtableSinkWriter<T> implements SinkWriter<T> {
    private final BigtableFlushableWriter writer;
    private final BaseSerializer<T> serializer;

    public BigtableSinkWriter(BigtableFlushableWriter writer, BaseSerializer<T> serializer)
            throws IOException {
        this.writer = writer;
        this.serializer = serializer;
    }

    /** Serializes and collects elements. */
    @Override
    public void write(T element, Context context) throws InterruptedException {
        writer.collect(serializer.serialize(element, context));
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
}
