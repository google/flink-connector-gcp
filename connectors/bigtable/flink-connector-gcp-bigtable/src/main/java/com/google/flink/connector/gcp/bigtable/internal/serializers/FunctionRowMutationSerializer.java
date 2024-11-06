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

package com.google.flink.connector.gcp.bigtable.internal.serializers;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.function.SerializableFunction;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;

/**
 * A serializer that converts elements of type {@code T} into Bigtable {@link RowMutationEntry}
 * objects using a provided function.
 *
 * <p>This serializer allows users to define custom serialization logic by providing a {@link
 * SerializableFunction} that maps elements to {@link RowMutationEntry} objects.
 *
 * @param <T> The type of elements to be serialized.
 */
public class FunctionRowMutationSerializer<T> implements BaseRowMutationSerializer<T> {

    private final SerializableFunction<T, RowMutationEntry> function;

    public FunctionRowMutationSerializer(SerializableFunction<T, RowMutationEntry> function) {
        this.function = function;
    }

    @Override
    public RowMutationEntry serialize(T element, SinkWriter.Context context) {
        return function.apply(element);
    }
}
