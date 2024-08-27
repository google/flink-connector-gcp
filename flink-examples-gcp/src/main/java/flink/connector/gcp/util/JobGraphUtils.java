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

package flink.connector.gcp.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorProvider;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.util.SerializedValue;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamWriteConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

/**
 * Flink JobGraph Json Serializer.
 */
public class JobGraphUtils {

  public static JobGraph deserializeJobGraph(File jobGraphFile)
      throws IOException, ClassNotFoundException {
    try (ObjectInputStream objectIn = new ObjectInputStream(new FileInputStream(jobGraphFile))) {
      return (JobGraph) objectIn.readObject();
    }
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: JobGraphUtils <job_graph_file>");
      System.exit(1);
    }

    File jobGraphFile = new File(args[0]);
    try {
      JobGraph jobGraph = deserializeJobGraph(jobGraphFile);
      // Process the deserialized JobGraph here
      System.out.println("Deserialized JobGraph: " + jobGraph);

      ObjectMapper mapper = new ObjectMapper();

      SimpleModule module = new SimpleModule();
      module.addSerializer(Path.class, new StringSerializer());
      module.addSerializer(SlotSharingGroup.class, new StringSerializer());
      module.addSerializer(org.apache.flink.runtime.jobgraph.JobEdge.class,
           new StringSerializer());
      module.addSerializer(org.apache.flink.api.common.operators.ResourceSpec.class,
          new StringSerializer());
      module.addSerializer(org.apache.flink.runtime.jobgraph.IntermediateDataSet.class,
          new StringSerializer());
      module.addSerializer(org.apache.flink.api.common.serialization.SerializerConfig.class,
          new StringSerializer());
      module.addSerializer(org.apache.flink.configuration.Configuration.class,
          new ConfigurationSerializer());
      module.addSerializer(SerializedValue.class, new SerializedValueStringSerializer());
      mapper.registerModule(module);
      mapper.getFactory().setStreamWriteConstraints(
          StreamWriteConstraints.builder().maxNestingDepth(10000).build());

      String json = mapper.writeValueAsString(jobGraph);
      System.out.println(json);

    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  static class StringSerializer extends StdSerializer<Object> {

    public StringSerializer() {
      super(Object.class);
    }

    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider provider)
        throws IOException, JsonProcessingException {
      gen.writeString(value.toString());

    }
  }

  static class SerializedValueStringSerializer extends StdSerializer<SerializedValue> {

    public SerializedValueStringSerializer() {
      super(SerializedValue.class);
    }

    @Override
    public void serialize(SerializedValue value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      Object deserializedValue = null;
      // Serialize the ExecutionConfig as JSON
      ObjectMapper objectMapper = new ObjectMapper();
      SimpleModule module = new SimpleModule();
      module.addSerializer(Path.class, new StringSerializer());
      module.addSerializer(SlotSharingGroup.class, new StringSerializer());
      module.addSerializer(ExecutionConfig.GlobalJobParameters.class, new StringSerializer());
      module.addSerializer(org.apache.flink.runtime.jobgraph.JobVertex.class,
          new StringSerializer());
      module.addSerializer(org.apache.flink.api.common.serialization.SerializerConfig.class,
          new StringSerializer());
      module.addSerializer(org.apache.flink.configuration.Configuration.class,
          new ConfigurationSerializer());
      module.addSerializer(SerializedValue.class, new SerializedValueStringSerializer());
      objectMapper.registerModule(module);
      objectMapper.getFactory().setStreamWriteConstraints(
          StreamWriteConstraints.builder().maxNestingDepth(10000).build());

      try {
        deserializedValue = value.deserializeValue(this.getClass().getClassLoader());
        if (deserializedValue instanceof ExecutionConfig) {
          ExecutionConfig executionConfig = (ExecutionConfig) deserializedValue;
          String jsonString = objectMapper.writeValueAsString(executionConfig);
          gen.writeString(jsonString);
          //gen.writeString(executionConfig.toString());

        } else if (deserializedValue instanceof FileSystemCheckpointStorage) {
          FileSystemCheckpointStorage executionConfig = (FileSystemCheckpointStorage) deserializedValue;
          String jsonString = objectMapper.writeValueAsString(executionConfig);
          gen.writeString(jsonString);
        } else if (deserializedValue instanceof SourceCoordinatorProvider) {
          SourceCoordinatorProvider executionConfig = (SourceCoordinatorProvider) deserializedValue;
          String jsonString = objectMapper.writeValueAsString(executionConfig);
          gen.writeString(jsonString);
        } else {
          // Handle other types as needed
          throw new RuntimeException("Unsupported type: " + deserializedValue.getClass().getName());
        }
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static class ConfigurationSerializer extends StdSerializer<Configuration> {

    public ConfigurationSerializer() {
      super(Configuration.class);
    }

    @Override
    public void serialize(Configuration value, JsonGenerator gen, SerializerProvider provider)
        throws IOException, JsonProcessingException {
      gen.writeStartObject();
      for (Map.Entry<String, String> entry : value.toMap().entrySet()) {
        gen.writeStringField(entry.getKey(), entry.getValue());
      }
      gen.writeEndObject();
    }
  }
}
