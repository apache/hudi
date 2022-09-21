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

package org.apache.hudi.secondary.index;

import org.roaringbitmap.RoaringBitmap;

public class RowIdSet implements IRowIdSet {
  protected final RoaringBitmap rowIdSet;

  public RowIdSet() {
    this.rowIdSet = new RoaringBitmap();
  }

  public RowIdSet(RoaringBitmap rowIdSet) {
    this.rowIdSet = rowIdSet;
  }

  @Override
  public void add(int rowId) {
    rowIdSet.add(rowId);
  }

  @Override
  public boolean get(int rowId) {
    return rowIdSet.contains(rowId);
  }

  @Override
  public IRowIdSetIterator iterator() {
    return new RowIdSetIterator(rowIdSet.getIntIterator());
  }

  @Override
  public int cardinality() {
    return rowIdSet.getCardinality();
  }

  @Override
  public Object getContainer() {
    return rowIdSet;
  }

  @Override
  public IRowIdSet and(IRowIdSet other) {
    return new RowIdSet(RoaringBitmap.and(rowIdSet, (RoaringBitmap) other.getContainer()));
  }

  @Override
  public IRowIdSet or(IRowIdSet other) {
    return new RowIdSet(RoaringBitmap.or(rowIdSet, (RoaringBitmap) other.getContainer()));
  }

  @Override
  public IRowIdSet not(IRowIdSet other) {
    return new RowIdSet(RoaringBitmap.andNot(rowIdSet, (RoaringBitmap) other.getContainer()));
  }
}
