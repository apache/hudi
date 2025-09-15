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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.SeekableDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An implementation a {@link HFileReader}.
 */
public class HFileReaderImpl extends BaseHFileReaderImpl {

  public HFileReaderImpl(SeekableDataInputStream stream, long fileSize) {
    super(stream, fileSize);
  }

  @Override
  public synchronized void initializeMetadata() throws IOException {
    super.initializeMetadata();
  }

  @Override
  public Option<byte[]> getMetaInfo(UTF8StringKey key) throws IOException {
    return super.getMetaInfo(key);
  }

  @Override
  public Option<ByteBuffer> getMetaBlock(String metaBlockName) throws IOException {
    return super.getMetaBlock(metaBlockName);
  }

  @Override
  public long getNumKeyValueEntries() {
    return super.getNumKeyValueEntries();
  }

  @Override
  public int seekTo(Key key) throws IOException {
    return super.seekTo(key);
  }

  @Override
  public boolean seekTo() throws IOException {
    return super.seekTo();
  }

  @Override
  public boolean next() throws IOException {
    return super.next();
  }

  @Override
  public Option<KeyValue> getKeyValue() throws IOException {
    return super.getKeyValue();
  }

  @Override
  public boolean isSeeked() {
    return super.isSeeked();
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}