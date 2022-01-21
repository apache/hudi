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

import org.apache.yetus.audience.InterfaceAudience;

/**
 *  A generic class for pair of an Object and and a primitive int value.
 */
@InterfaceAudience.Private
public class ObjectIntPair<T> {

  private T first;
  private int second;

  public ObjectIntPair() {
  }

  public ObjectIntPair(T first, int second) {
    this.setFirst(first);
    this.setSecond(second);
  }

  public T getFirst() {
    return first;
  }

  public void setFirst(T first) {
    this.first = first;
  }

  public int getSecond() {
    return second;
  }

  public void setSecond(int second) {
    this.second = second;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof ObjectIntPair && equals(first, ((ObjectIntPair<?>) other).first)
        && (this.second == ((ObjectIntPair<?>) other).second);
  }

  private static boolean equals(Object x, Object y) {
    return (x == null && y == null) || (x != null && x.equals(y));
  }

  @Override
  public int hashCode() {
    return first == null ? 0 : (first.hashCode() * 17) + 13 * second;
  }

  @Override
  public String toString() {
    return "{" + getFirst() + "," + getSecond() + "}";
  }
}
