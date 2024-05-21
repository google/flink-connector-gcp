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

package flink.contector.gcp;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

public class AvroRandomRecordGenerator {

  public static String getRandomString() {
    return RandomStringUtils.randomAlphabetic(RandomUtils.nextInt(2, 40));
  }

  public static Integer getRandomInteger() {
    return RandomUtils.nextInt(0, Integer.MAX_VALUE);
  }

  public static Long getRandomLong() {
    return RandomUtils.nextLong(0L, Long.MAX_VALUE);
  }

  public static Double getRandomDouble() {
    return RandomUtils.nextDouble(0, Double.MAX_VALUE);
  }

  public static Float getRandomFloat() {
    return RandomUtils.nextFloat(0f, Float.MAX_VALUE);
  }

  public static Boolean getRandomBoolean() {
    return RandomUtils.nextInt(0, 10) % 2 == 0 ? Boolean.TRUE : Boolean.FALSE;
  }

  public static ByteBuffer getRandomBytes() {
    return ByteBuffer.wrap(RandomUtils.nextBytes(RandomUtils.nextInt(1, 25)));
  }

  /**
   * Generates Generic Record with random values to given avro Schema.
   *
   * @param schema Avro Schema
   * @return
   */
  public static GenericData.Record generateRandomRecordData(Schema schema) {
    if (!Schema.Type.RECORD.equals(schema.getType())) {
      throw new IllegalArgumentException("input schema must be a record schema");
    }

    final GenericData.Record record = new GenericData.Record(schema);

    for (Schema.Field field : schema.getFields()) {
      switch (field.schema().getType()) {
        case BOOLEAN:
          record.put(field.pos(), getRandomBoolean());
          break;
        case INT:
          record.put(field.pos(), getRandomInteger());
          break;
        case LONG:
          record.put(field.pos(), getRandomLong());
          break;
        case DOUBLE:
          record.put(field.pos(), getRandomDouble());
          break;
        case FLOAT:
          record.put(field.pos(), getRandomFloat());
          break;
        case BYTES:
          record.put(field.pos(), getRandomBytes());
          break;
        case STRING:
          record.put(field.pos(), getRandomString());
          break;
        case RECORD:
          record.put(field.pos(), generateRandomRecordData(field.schema()));
          break;
        case ENUM:
          record.put(field.pos(), generateRandomEnumSymbol(field.schema()));
          break;
        case FIXED:
          record.put(field.pos(), generateRandomFixed(field.schema()));
          break;
        case UNION:
          record.put(field.pos(), generateRandomUnion(field.schema()));
          break;
        case ARRAY:
          record.put(field.pos(), generateRandomArray(field.schema()));
          break;
        case MAP:
          record.put(field.pos(), generateRandomMap(field.schema()));
          break;
        default:
          throw new IllegalArgumentException("Not Support type " + schema.getValueType().getType());
      }
    }

    return record;
  }

  /**
   * Generates Enum Field with random values to given avro Schema.
   *
   * @param schema Avro Schema
   * @return
   */
  public static GenericData.EnumSymbol generateRandomEnumSymbol(Schema schema) {
    if (!Schema.Type.ENUM.equals(schema.getType())) {
      throw new IllegalArgumentException("input schema must be an enum schema");
    }

    return new GenericData.EnumSymbol(schema,
        schema.getEnumSymbols().get(RandomUtils.nextInt(0, schema.getEnumSymbols().size())));
  }

  /**
   * Generates Fixed Field with random values to given avro Schema.
   *
   * @param schema Avro Schema
   * @return
   */
  public static GenericData.Fixed generateRandomFixed(Schema schema) {
    if (!Schema.Type.FIXED.equals(schema.getType())) {
      throw new IllegalArgumentException("input schema must be an fixed schema");
    }

    return new GenericData.Fixed(schema,
        RandomUtils.nextBytes(schema.getFixedSize()));
  }

  /**
   * Generates Union Field with random values to given avro Schema.
   *
   * @param schema Avro Schema
   * @return
   */
  public static Object generateRandomUnion(Schema schema) {
    if (!Schema.Type.UNION.equals(schema.getType())) {
      throw new IllegalArgumentException("input schema must be an union schema");
    }
    Object unionData = null;
    switch (schema.getTypes().get(1).getType()) {
      case BOOLEAN:
        unionData = getRandomBoolean();
        break;
      case INT:
        unionData = getRandomInteger();
        break;
      case LONG:
        unionData = getRandomLong();
        break;
      case DOUBLE:
        unionData = getRandomDouble();
        break;
      case FLOAT:
        unionData = getRandomFloat();
        break;
      case BYTES:
        unionData = getRandomBytes();
        break;
      case STRING:
        unionData = getRandomString();
        break;
      case RECORD:
        unionData = generateRandomRecordData(schema.getTypes().get(1));
        break;
      case ENUM:
        unionData = generateRandomEnumSymbol(schema.getTypes().get(1));
        break;
      case FIXED:
        unionData = generateRandomFixed(schema.getTypes().get(1));
        break;
      case UNION:
        unionData = generateRandomUnion(schema.getTypes().get(1));
        break;
      default:
        unionData = null;
    }

    return unionData;
  }

  /**
   * Generates Array Field with random values to given avro Schema.
   *
   * @param schema Avro Schema
   * @return
   */
  public static GenericData.Array<?> generateRandomArray(Schema schema) {
    if (!Schema.Type.ARRAY.equals(schema.getType())) {
      throw new IllegalArgumentException("input schema must be an array schema");
    }

    int elements = RandomUtils.nextInt(1, 11);
    GenericData.Array<Object> arrayData = new GenericData.Array<>(elements, schema);
    for (int i = 0; i < elements; i++) {
      switch (schema.getElementType().getType()) {
        case BOOLEAN:
          arrayData.add(getRandomBoolean());
          break;
        case INT:
          arrayData.add(getRandomInteger());
          break;
        case LONG:
          arrayData.add(getRandomLong());
          break;
        case DOUBLE:
          arrayData.add(getRandomDouble());
          break;
        case FLOAT:
          arrayData.add(getRandomFloat());
          break;
        case BYTES:
          arrayData.add(getRandomBytes());
          break;
        case STRING:
          arrayData.add(getRandomString());
          break;
        case RECORD:
          arrayData.add(generateRandomRecordData(schema.getElementType()));
          break;
        case ENUM:
          arrayData.add(generateRandomEnumSymbol(schema.getElementType()));
          break;
        case FIXED:
          arrayData.add(generateRandomFixed(schema.getElementType()));
          break;
        default:
          throw new IllegalArgumentException("Not Support type " + schema.getValueType().getType());
      }
    }

    return arrayData;
  }

  /**
   * Generates Map Field with random values to given avro Schema.
   *
   * @param schema Avro Schema
   * @return
   */
  public static Map<String, ?> generateRandomMap(Schema schema) {
    if (!Schema.Type.MAP.equals(schema.getType())) {
      throw new IllegalArgumentException("input schema must be an map schema");
    }

    int elements = RandomUtils.nextInt(1, 11);
    Map<String, Object> mapData = new HashMap<>();
    for (int i = 0; i < elements; i++) {
      switch (schema.getValueType().getType()) {
        case BOOLEAN:
          mapData.put(RandomStringUtils.randomAlphabetic(10), getRandomBoolean());
          break;
        case INT:
          mapData.put(RandomStringUtils.randomAlphabetic(10), getRandomInteger());
          break;
        case LONG:
          mapData.put(RandomStringUtils.randomAlphabetic(10), getRandomLong());
          break;
        case DOUBLE:
          mapData.put(RandomStringUtils.randomAlphabetic(10), getRandomDouble());
          break;
        case FLOAT:
          mapData.put(RandomStringUtils.randomAlphabetic(10), getRandomFloat());
          break;
        case BYTES:
          mapData.put(RandomStringUtils.randomAlphabetic(10), getRandomBytes());
          break;
        case STRING:
          mapData.put(RandomStringUtils.randomAlphabetic(10), getRandomString());
          break;
        case RECORD:
          mapData.put(RandomStringUtils.randomAlphabetic(10),
              generateRandomRecordData(schema.getValueType()));
          break;
        case ENUM:
          mapData.put(RandomStringUtils.randomAlphabetic(10),
              generateRandomEnumSymbol(schema.getValueType()));
          break;
        case FIXED:
          mapData.put(RandomStringUtils.randomAlphabetic(10),
              generateRandomFixed(schema.getValueType()));
          break;
        default:
          throw new IllegalArgumentException("Not Support type " + schema.getValueType().getType());
      }
    }

    return mapData;
  }

}