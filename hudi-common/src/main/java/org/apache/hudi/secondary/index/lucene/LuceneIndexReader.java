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

import org.apache.hudi.secondary.index.HoodieSecondaryIndex;
import org.apache.hudi.secondary.index.SecondaryIndexReader;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

public class LuceneIndexReader implements SecondaryIndexReader {
  private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexReader.class);

  private String indexPath;
  private IndexSearcher indexSearcher;

  public LuceneIndexReader(String indexPath) throws IOException {
    this.indexPath = indexPath;
    FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
    indexSearcher = new IndexSearcher(DirectoryReader.open(directory));
  }

  @Override
  public SecondaryIndexReader open(HoodieSecondaryIndex secondaryIndex, String indexPath) {
    return null;
  }

  @Override
  public RoaringBitmap queryTerm() {
    return null;
  }
}
