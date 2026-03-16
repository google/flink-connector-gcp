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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Serializer for the enumerator checkpoint state (a collection of unassigned splits).
 *
 * <p>Delegates to {@link BigtableChangeStreamSplitSerializer} for individual splits.
 */
public final class BigtableChangeStreamEnumeratorCheckpointSerializer
        implements SimpleVersionedSerializer<Collection<BigtableChangeStreamSplit>> {

    public static final BigtableChangeStreamEnumeratorCheckpointSerializer INSTANCE =
            new BigtableChangeStreamEnumeratorCheckpointSerializer();

    private static final int VERSION = 1;

    private BigtableChangeStreamEnumeratorCheckpointSerializer() {}

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(Collection<BigtableChangeStreamSplit> splits) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeInt(splits.size());
        for (BigtableChangeStreamSplit split : splits) {
            byte[] splitBytes = BigtableChangeStreamSplitSerializer.INSTANCE.serialize(split);
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }

        out.flush();
        return baos.toByteArray();
    }

    @Override
    public Collection<BigtableChangeStreamSplit> deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported BigtableChangeStreamEnumeratorCheckpointSerializer version: "
                            + version);
        }

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));
        int count = in.readInt();
        if (count < 0 || count > serialized.length) {
            throw new IOException(
                    String.format(
                            "Invalid split count %d (total serialized bytes: %d)",
                            count, serialized.length));
        }

        Collection<BigtableChangeStreamSplit> splits = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            int len = in.readInt();
            if (len < 0 || len > serialized.length) {
                throw new IOException(
                        String.format(
                                "Invalid split byte length %d at index %d "
                                        + "(total serialized bytes: %d)",
                                len, i, serialized.length));
            }
            byte[] splitBytes = new byte[len];
            in.readFully(splitBytes);
            splits.add(
                    BigtableChangeStreamSplitSerializer.INSTANCE.deserialize(
                            BigtableChangeStreamSplitSerializer.INSTANCE.getVersion(), splitBytes));
        }

        return splits;
    }
}
