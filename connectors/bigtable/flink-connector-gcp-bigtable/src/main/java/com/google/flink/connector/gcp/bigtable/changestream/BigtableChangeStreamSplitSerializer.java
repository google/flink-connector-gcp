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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Serializer for {@link BigtableChangeStreamSplit}. */
public final class BigtableChangeStreamSplitSerializer
        implements SimpleVersionedSerializer<BigtableChangeStreamSplit> {

    public static final BigtableChangeStreamSplitSerializer INSTANCE =
            new BigtableChangeStreamSplitSerializer();

    private static final int VERSION = 1;

    private BigtableChangeStreamSplitSerializer() {}

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(BigtableChangeStreamSplit split) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        byte[] start = split.getPartition().getStart().toByteArray();
        byte[] end = split.getPartition().getEnd().toByteArray();

        out.writeInt(start.length);
        out.write(start);
        out.writeInt(end.length);
        out.write(end);

        String token = split.getContinuationToken();
        if (token != null) {
            out.writeBoolean(true);
            byte[] tokenBytes = token.getBytes(StandardCharsets.UTF_8);
            out.writeInt(tokenBytes.length);
            out.write(tokenBytes);
        } else {
            out.writeBoolean(false);
        }

        out.flush();
        return baos.toByteArray();
    }

    @Override
    public BigtableChangeStreamSplit deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported BigtableChangeStreamSplitSerializer version: " + version);
        }

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));

        int startLen = in.readInt();
        byte[] start = new byte[startLen];
        in.readFully(start);

        int endLen = in.readInt();
        byte[] end = new byte[endLen];
        in.readFully(end);

        ByteStringRange partition =
                ByteStringRange.create(ByteString.copyFrom(start), ByteString.copyFrom(end));

        String token = null;
        if (in.readBoolean()) {
            int tokenLen = in.readInt();
            byte[] tokenBytes = new byte[tokenLen];
            in.readFully(tokenBytes);
            token = new String(tokenBytes, StandardCharsets.UTF_8);
        }

        return new BigtableChangeStreamSplit(partition, token);
    }
}
