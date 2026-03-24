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

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Compatibility utility for ParameterTool which moved in Flink 2.0.
 */
public class ParameterToolCompat {

  private static final Logger LOG = Logger.getLogger(ParameterToolCompat.class.getName());

  public static Object fromArgs(String[] args) {
    try {
      return getParameterToolClass().getMethod("fromArgs", String[].class).invoke(null, (Object) args);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to invoke fromArgs", e);
      throw new RuntimeException(e);
    }
  }

  public static Object fromArgsMultiple(String[] args) {
    try {
      return getMultipleParameterToolClass().getMethod("fromArgs", String[].class).invoke(null, (Object) args);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to invoke fromArgs for MultipleParameterTool", e);
      throw new RuntimeException(e);
    }
  }

  private static Class<?> getParameterToolClass() {
    try {
      return Class.forName("org.apache.flink.api.java.utils.ParameterTool");
    } catch (ClassNotFoundException e) {
      try {
        return Class.forName("org.apache.flink.util.ParameterTool");
      } catch (ClassNotFoundException e1) {
        throw new RuntimeException("ParameterTool class not found", e1);
      }
    }
  }

  private static Class<?> getMultipleParameterToolClass() {
    try {
      return Class.forName("org.apache.flink.api.java.utils.MultipleParameterTool");
    } catch (ClassNotFoundException e) {
      try {
        return Class.forName("org.apache.flink.util.MultipleParameterTool");
      } catch (ClassNotFoundException e1) {
        throw new RuntimeException("MultipleParameterTool class not found", e1);
      }
    }
  }

  public static String get(Object parameterTool, String key) {
    try {
      return (String) parameterTool.getClass().getMethod("get", String.class).invoke(parameterTool, key);
    } catch (Exception e) {
       throw new RuntimeException(e);
    }
  }

  public static String get(Object parameterTool, String key, String defaultValue) {
    try {
      return (String) parameterTool.getClass().getMethod("get", String.class, String.class).invoke(parameterTool, key, defaultValue);
    } catch (Exception e) {
       throw new RuntimeException(e);
    }
  }

  public static int getInt(Object parameterTool, String key, int defaultValue) {
    try {
      return (Integer) parameterTool.getClass().getMethod("getInt", String.class, int.class).invoke(parameterTool, key, defaultValue);
    } catch (Exception e) {
       throw new RuntimeException(e);
    }
  }

  public static long getLong(Object parameterTool, String key, long defaultValue) {
    try {
      return (Long) parameterTool.getClass().getMethod("getLong", String.class, long.class).invoke(parameterTool, key, defaultValue);
    } catch (Exception e) {
       throw new RuntimeException(e);
    }
  }

  public static boolean getBoolean(Object parameterTool, String key, boolean defaultValue) {
    try {
      return (Boolean) parameterTool.getClass().getMethod("getBoolean", String.class, boolean.class).invoke(parameterTool, key, defaultValue);
    } catch (Exception e) {
       throw new RuntimeException(e);
    }
  }
}
