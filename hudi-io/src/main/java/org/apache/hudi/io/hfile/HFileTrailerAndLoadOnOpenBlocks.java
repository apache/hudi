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

package org.apache.hudi.io.hfile;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * The parts of an HFile a reader needs before it can read any data or meta block: the trailer
 * and the three blocks of the "load-on-open" section the trailer points to.
 *
 * <p>They are cached as one unit because they are read together when a file is opened and are
 * all required to initialize the reader.
 */
public final class HFileTrailerAndLoadOnOpenBlocks {
  final HFileTrailer trailer;
  final HFileRootIndexBlock rootDataIndexBlock;
  final HFileRootIndexBlock metaRootIndexBlock;
  final HFileFileInfoBlock fileInfoBlock;

  public HFileTrailerAndLoadOnOpenBlocks(HFileTrailer trailer,
                                         HFileRootIndexBlock rootDataIndexBlock,
                                         HFileRootIndexBlock metaRootIndexBlock,
                                         HFileFileInfoBlock fileInfoBlock) {
    this.trailer = trailer;
    this.rootDataIndexBlock = rootDataIndexBlock;
    this.metaRootIndexBlock = metaRootIndexBlock;
    this.fileInfoBlock = fileInfoBlock;
  }

  /**
   * Returns the bytes this entry keeps alive for byte-weighted caching. The three blocks are
   * slices of the one array read for the whole "load-on-open" section, so each distinct array
   * is counted once.
   */
  long heapSize() {
    Set<byte[]> buffers = Collections.newSetFromMap(new IdentityHashMap<>());
    for (HFileBlock block : Arrays.asList(rootDataIndexBlock, metaRootIndexBlock, fileInfoBlock)) {
      buffers.addAll(block.retainedBuffers());
    }
    long size = 0L;
    for (byte[] buffer : buffers) {
      size += buffer.length;
    }
    return size;
  }
}
