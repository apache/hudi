/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hbase.util;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.gson.GsonBuilder;
import org.apache.hbase.thirdparty.com.google.gson.LongSerializationPolicy;
import org.apache.hbase.thirdparty.com.google.gson.TypeAdapter;
import org.apache.hbase.thirdparty.com.google.gson.stream.JsonReader;
import org.apache.hbase.thirdparty.com.google.gson.stream.JsonWriter;

/**
 * Helper class for gson.
 */
@InterfaceAudience.Private
public final class GsonUtil {

  private GsonUtil() {
  }

  /**
   * Create a builder which is used to create a Gson instance.
   * <p/>
   * Will set some common configs for the builder.
   */
  public static GsonBuilder createGson() {
    return new GsonBuilder().setLongSerializationPolicy(LongSerializationPolicy.STRING)
        .registerTypeAdapter(LongAdder.class, new TypeAdapter<LongAdder>() {

          @Override
          public void write(JsonWriter out, LongAdder value) throws IOException {
            out.value(value.longValue());
          }

          @Override
          public LongAdder read(JsonReader in) throws IOException {
            LongAdder value = new LongAdder();
            value.add(in.nextLong());
            return value;
          }
        });
  }

  public static GsonBuilder createGsonWithDisableHtmlEscaping() {
    return createGson().disableHtmlEscaping();
  }
}
