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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterConnectionHandler;
import redis.clients.jedis.JedisClusterInfoCache;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSlotBasedConnectionHandler;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.jedis.exceptions.JedisRedirectionException;
import redis.clients.util.JedisClusterCRC16;
import redis.clients.util.SafeEncoder;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * An implementation of {@link Pipeline} for redis cluster mode.
 */
public class JedisClusterPipeline extends Pipeline implements Closeable {
  private static final Logger LOG = LogManager.getLogger(JedisClusterPipeline.class);

  private static final Field FIELD_CONNECTION_HANDLER;
  private static final Field FIELD_CACHE;

  static {
    FIELD_CONNECTION_HANDLER = getField(BinaryJedisCluster.class, "connectionHandler");
    FIELD_CACHE = getField(JedisClusterConnectionHandler.class, "cache");
  }

  private JedisSlotBasedConnectionHandler connectionHandler;
  private JedisClusterInfoCache clusterInfoCache;
  private Queue<Client> clients = new LinkedList<>();
  private Map<JedisPool, Jedis> jedisMap = new HashMap<>();
  private boolean hasDataInBuf = false;

  public JedisClusterPipeline(JedisCluster jedis) {
    LOG.info(FIELD_CONNECTION_HANDLER.getName());
    LOG.info(FIELD_CACHE.getName());
    LOG.info(jedis.toString());
    connectionHandler = getValue(jedis, FIELD_CONNECTION_HANDLER);
    clusterInfoCache = getValue(connectionHandler, FIELD_CACHE);
  }

  public void refreshCluster() {
    connectionHandler.renewSlotCache();
  }

  @Override
  public void sync() {
    innerSync(null);
  }

  @Override
  public List<Object> syncAndReturnAll() {
    List<Object> responseList = new ArrayList<>();
    innerSync(responseList);
    return responseList;
  }

  private void innerSync(List<Object> formatted) {
    try {
      for (Client client : clients) {
        List<Object> results = client.getAll();
        for (Object res : results) {
          Object data = generateResponse(res).get();
          if (null != formatted) {
            formatted.add(data);
          }
        }
      }
    } catch (JedisRedirectionException e) {
      if (e instanceof JedisMovedDataException) {
        refreshCluster();
      }
      throw e;
    } finally {
      for (Jedis jedis : jedisMap.values()) {
        flushCachedData(jedis);
      }
      hasDataInBuf = false;
      close();
    }
  }

  @Override
  public void close() {
    clean();
    clients.clear();
    for (Jedis jedis : jedisMap.values()) {
      if (hasDataInBuf) {
        flushCachedData(jedis);
      }
      jedis.close();
    }
    jedisMap.clear();
    hasDataInBuf = false;
  }

  private void flushCachedData(Jedis jedis) {
    try {
      jedis.getClient().getAll();
    } catch (RuntimeException ex) {
      // 其中一个client出问题，后面出问题的几率较大
    }
  }

  @Override
  protected Client getClient(String key) {
    byte[] bKey = SafeEncoder.encode(key);
    return getClient(bKey);
  }

  @Override
  protected Client getClient(byte[] key) {
    Jedis jedis = getJedis(JedisClusterCRC16.getSlot(key));
    Client client = jedis.getClient();
    clients.add(client);
    return client;
  }

  private Jedis getJedis(int slot) {
    JedisPool pool = clusterInfoCache.getSlotPool(slot);
    Jedis jedis = jedisMap.get(pool);
    if (null == jedis) {
      jedis = pool.getResource();
      jedisMap.put(pool, jedis);
    }
    hasDataInBuf = true;
    return jedis;
  }

  private static Field getField(Class<?> cls, String fieldName) {
    try {
      Field field = cls.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field;
    } catch (NoSuchFieldException | SecurityException e) {
      throw new RuntimeException("cannot find or access field '" + fieldName + "' from " + cls.getName(), e);
    }
  }

  @SuppressWarnings({"unchecked"})
  private static <T> T getValue(Object obj, Field field) {
    try {
      return (T) field.get(obj);
    } catch (IllegalArgumentException | IllegalAccessException e) {
      LOG.error("get value fail", e);
      throw new RuntimeException(e);
    }
  }
}
