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

package org.apache.hudi.index.redis;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

/**
 * A common interface for both standalone and cluster mode.
 */
public interface RedisClient extends AutoCloseable, Serializable {

  String OK = "OK";

  /**
   * Returns true if the given key exists.
   *
   * @param key The key whose existence is tested.
   * @return True if the given key exists.
   */
  Boolean exists(byte[] key);

  /**
   * Returns the value associated with the given key.
   *
   * @param key The key whose value is retrieved.
   * @return The value associated with the given key.
   */
  byte[] get(byte[] key);

  /**
   * Associates the given key with the given value.
   *
   * @param key        The key to be updated.
   * @param value      The value associated with the key.
   * @param expireTime The expire time of the key.
   */
  void set(byte[] key, byte[] value, Duration expireTime);

  /**
   * Deletes the given key.
   *
   * @param key The key to be deleted.
   */
  void delete(byte[] key);

  /**
   * Returns values associated with a list of keys.
   *
   * @param keys Array of keys for query.
   * @return Key-value map of the query result.
   */
  Map<byte[], byte[]> batchGet(byte[][] keys);
}
