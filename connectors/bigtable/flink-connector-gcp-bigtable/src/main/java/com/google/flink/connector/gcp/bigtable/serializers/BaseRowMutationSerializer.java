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

package com.google.flink.connector.gcp.bigtable.serializers;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;

import java.io.Serializable;

/**
 * Base serializer class.
 *
 * <p>This class implements the interface to serialize to {@link RowMutationEntry}.
 */
public interface BaseRowMutationSerializer<T> extends Serializable {
    RowMutationEntry serialize(T element, SinkWriter.Context context);
}
