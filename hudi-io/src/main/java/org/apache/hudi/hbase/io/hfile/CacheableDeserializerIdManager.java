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

package org.apache.hudi.hbase.io.hfile;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * This class is used to manage the identifiers for {@link CacheableDeserializer}.
 * All deserializers are registered with this Manager via the
 * {@link #registerDeserializer(CacheableDeserializer)}}. On registration, we return an
 * int *identifier* for this deserializer. The int identifier is passed to
 * {@link #getDeserializer(int)}} to obtain the registered deserializer instance.
 */
@InterfaceAudience.Private
public class CacheableDeserializerIdManager {
  private static final Map<Integer, CacheableDeserializer<Cacheable>> registeredDeserializers = new HashMap<>();
  private static final AtomicInteger identifier = new AtomicInteger(0);

  /**
   * Register the given {@link Cacheable} -- usually an hfileblock instance, these implement
   * the Cacheable Interface -- deserializer and generate a unique identifier id for it and return
   * this as our result.
   * @return the identifier of given cacheable deserializer
   * @see #getDeserializer(int)
   */
  public static int registerDeserializer(CacheableDeserializer<Cacheable> cd) {
    int idx = identifier.incrementAndGet();
    synchronized (registeredDeserializers) {
      registeredDeserializers.put(idx, cd);
    }
    return idx;
  }

  /**
   * Get the cacheable deserializer registered at the given identifier Id.
   * @see #registerDeserializer(CacheableDeserializer)
   */
  public static CacheableDeserializer<Cacheable> getDeserializer(int id) {
    return registeredDeserializers.get(id);
  }

  /**
   * Snapshot a map of the current identifiers to class names for reconstruction on reading out
   * of a file.
   */
  public static Map<Integer,String> save() {
    Map<Integer, String> snapshot = new HashMap<>();
    synchronized (registeredDeserializers) {
      for (Map.Entry<Integer, CacheableDeserializer<Cacheable>> entry :
          registeredDeserializers.entrySet()) {
        snapshot.put(entry.getKey(), entry.getValue().getClass().getName());
      }
    }
    return snapshot;
  }
}
