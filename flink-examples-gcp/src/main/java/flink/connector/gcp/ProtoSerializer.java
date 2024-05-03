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

package flink.connector.gcp;

import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * ProtoSerializer class.
 */
public class ProtoSerializer extends BigQueryProtoSerializer<GenericRecord> {

    @Override
    public ByteString serialize(GenericRecord message) throws BigQuerySerializationException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(byteArrayOutputStream);

        // Assuming the field order in your MyMessage.proto is: word (1), countStr (2)
        try {
            codedOutputStream.writeString(1, (String) message.get(0));
            codedOutputStream.writeString(2, (String) message.get(1));
            codedOutputStream.flush(); // Ensure everything is written
        } catch (IOException e) {
            throw new BigQuerySerializationException(e.getMessage());
        }

        // Convert ByteArrayOutputStream to ByteString
        return ByteString.copyFrom(byteArrayOutputStream.toByteArray());
    }
}
