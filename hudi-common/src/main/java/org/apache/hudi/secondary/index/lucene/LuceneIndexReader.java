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

package org.apache.hudi.secondary.index.lucene;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieSecondaryIndexException;
import org.apache.hudi.secondary.index.HoodieSecondaryIndex;
import org.apache.hudi.secondary.index.SecondaryIndexReader;
import org.apache.hudi.secondary.index.lucene.hadoop.HdfsDirectory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LuceneIndexReader implements SecondaryIndexReader {
  private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexReader.class);

  private String indexPath;
  private DirectoryReader reader;
  private IndexSearcher searcher;

  public LuceneIndexReader(String indexPath, Configuration conf) {
    this.indexPath = indexPath;
    try {
      Path path = new Path(indexPath);
      String scheme = path.toUri().getScheme();
      if (!StringUtils.isNullOrEmpty(scheme)) {
        String disableCacheName = String.format("fs.%s.impl.disable.cache", scheme);
        conf.set(disableCacheName, "true");
      }
      Directory directory = new HdfsDirectory(path, conf);
      reader = DirectoryReader.open(directory);
    } catch (Exception e) {
      throw new HoodieSecondaryIndexException("Init lucene index reader failed", e);
    }
    searcher = new IndexSearcher(reader);
  }

  @Override
  public SecondaryIndexReader open(HoodieSecondaryIndex secondaryIndex, String indexPath) {
    return null;
  }

  @Override
  public RoaringBitmap queryTerm() {
    return null;
  }

  @Override
  public void close() {
    searcher = null;
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        LOG.error("Fail to close lucene index reader");
      }
    }
  }
}
