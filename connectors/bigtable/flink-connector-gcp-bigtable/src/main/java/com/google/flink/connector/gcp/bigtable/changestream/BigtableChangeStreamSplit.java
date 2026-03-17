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

import org.apache.flink.api.connector.source.SourceSplit;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;

import java.io.Serializable;
import java.util.Base64;

/**
 * A split representing a single Bigtable Change Stream partition.
 *
 * <p>Each split tracks its own continuation token, ensuring correct resume after checkpoints.
 */
public final class BigtableChangeStreamSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    private final ByteStringRange partition;
    private final String continuationToken;

    public BigtableChangeStreamSplit(ByteStringRange partition, String continuationToken) {
        this.partition = partition;
        this.continuationToken = continuationToken;
    }

    public ByteStringRange getPartition() {
        return partition;
    }

    /** Returns the continuation token, or {@code null} if this is a fresh split. */
    public String getContinuationToken() {
        return continuationToken;
    }

    /** Returns a new split with the updated continuation token. */
    public BigtableChangeStreamSplit withToken(String token) {
        return new BigtableChangeStreamSplit(partition, token);
    }

    @Override
    public String splitId() {
        byte[] start = partition.getStart().toByteArray();
        byte[] end = partition.getEnd().toByteArray();
        return Base64.getEncoder().encodeToString(start)
                + ":"
                + Base64.getEncoder().encodeToString(end);
    }

    @Override
    public String toString() {
        return String.format(
                "BigtableChangeStreamSplit{partition=%s, hasToken=%s}",
                splitId(), continuationToken != null);
    }
}
