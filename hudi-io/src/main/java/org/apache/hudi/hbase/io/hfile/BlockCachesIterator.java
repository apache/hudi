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

import java.util.Iterator;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Iterator over an array of BlockCache CachedBlocks.
 */
@InterfaceAudience.Private
class BlockCachesIterator implements Iterator<CachedBlock> {
  int index = 0;
  final BlockCache [] bcs;
  Iterator<CachedBlock> current;

  BlockCachesIterator(final BlockCache [] blockCaches) {
    this.bcs = blockCaches;
    this.current = this.bcs[this.index].iterator();
  }

  @Override
  public boolean hasNext() {
    if (current.hasNext()) return true;
    this.index++;
    if (this.index >= this.bcs.length) return false;
    this.current = this.bcs[this.index].iterator();
    return hasNext();
  }

  @Override
  public CachedBlock next() {
    return this.current.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
