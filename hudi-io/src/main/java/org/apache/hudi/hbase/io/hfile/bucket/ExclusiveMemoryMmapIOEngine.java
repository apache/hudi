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

package org.apache.hudi.hbase.io.hfile.bucket;

import java.io.IOException;

import org.apache.hudi.hbase.io.hfile.Cacheable;
import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * IO engine that stores data to a file on the local block device using memory mapping mechanism
 */
@InterfaceAudience.Private
public class ExclusiveMemoryMmapIOEngine extends FileMmapIOEngine {

  public ExclusiveMemoryMmapIOEngine(String filePath, long capacity) throws IOException {
    super(filePath, capacity);
  }

  @Override
  public Cacheable read(BucketEntry be) throws IOException {
    ByteBuff dst = be.allocator.allocate(be.getLength());
    bufferArray.read(be.offset(), dst);
    dst.position(0).limit(be.getLength());
    return be.wrapAsCacheable(dst);
  }
}
