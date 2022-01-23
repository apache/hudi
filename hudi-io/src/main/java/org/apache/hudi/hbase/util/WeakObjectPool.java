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

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A {@code WeakReference} based shared object pool.
 * The objects are kept in weak references and
 * associated with keys which are identified by the {@code equals} method.
 * The objects are created by {@link org.apache.hadoop.hbase.util.ObjectPool.ObjectFactory} on
 * demand. The object creation is expected to be lightweight, and the objects may be excessively
 * created and discarded.
 * Thread safe.
 */
@InterfaceAudience.Private
public class WeakObjectPool<K,V> extends ObjectPool<K,V> {

  public WeakObjectPool(ObjectFactory<K, V> objectFactory) {
    super(objectFactory);
  }

  public WeakObjectPool(ObjectFactory<K, V> objectFactory, int initialCapacity) {
    super(objectFactory, initialCapacity);
  }

  public WeakObjectPool(ObjectFactory<K, V> objectFactory, int initialCapacity,
                        int concurrencyLevel) {
    super(objectFactory, initialCapacity, concurrencyLevel);
  }

  @Override
  public Reference<V> createReference(K key, V obj) {
    return new WeakObjectReference(key, obj);
  }

  private class WeakObjectReference extends WeakReference<V> {
    final K key;

    WeakObjectReference(K key, V obj) {
      super(obj, staleRefQueue);
      this.key = key;
    }
  }

  @Override
  public K getReferenceKey(Reference<V> ref) {
    return ((WeakObjectReference)ref).key;
  }

}
