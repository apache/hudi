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
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.HoodieSecondaryIndexException;
import org.apache.hudi.internal.schema.Types.Field;
import org.apache.hudi.secondary.index.AllRowIdSet;
import org.apache.hudi.secondary.index.EmptyRowIdSet;
import org.apache.hudi.secondary.index.IRowIdSet;
import org.apache.hudi.secondary.index.ISecondaryIndexReader;
import org.apache.hudi.secondary.index.RowIdSet;
import org.apache.hudi.secondary.index.lucene.hadoop.HdfsDirectory;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import static org.apache.hudi.secondary.index.IndexConstants.NOT_NULL_FIELD;
import static org.apache.hudi.secondary.index.IndexConstants.NULL_FIELD;

public class LuceneIndexReader implements ISecondaryIndexReader {
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
  public IRowIdSet emptyRows() throws IOException {
    return new EmptyRowIdSet();
  }

  @Override
  public IRowIdSet allRows() throws IOException {
    try {
      reader.incRef();

      // RowId belongs to [0, maxDoc)
      return new AllRowIdSet(reader.maxDoc());
    } finally {
      reader.decRef();
    }
  }

  @Override
  public IRowIdSet queryNullTerm(Field field) throws IOException {
    try {
      reader.incRef();

      TermQuery query = new TermQuery(new Term(NULL_FIELD, field.name()));
      return search(query);
    } finally {
      reader.decRef();
    }
  }

  @Override
  public IRowIdSet queryNotNullTerm(Field field) throws IOException {
    try {
      reader.incRef();

      TermQuery query = new TermQuery(new Term(NOT_NULL_FIELD, field.name()));
      return search(query);
    } finally {
      reader.decRef();
    }
  }

  @Override
  public IRowIdSet queryTerm(Field field, Object value) throws IOException {
    try {
      reader.incRef();

      Query query = buildTermQuery(field, value);
      return search(query);
    } finally {
      reader.decRef();
    }
  }

  @Override
  public IRowIdSet queryTermList(Field field, List<Object> values) throws IOException {
    try {
      reader.incRef();

      BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
      for (Object value : values) {
        queryBuilder.add(buildTermQuery(field, value), BooleanClause.Occur.SHOULD);
      }
      return search(queryBuilder.build());
    } finally {
      reader.decRef();
    }
  }

  @Override
  public IRowIdSet queryPrefix(Field field, String prefixValue) throws IOException {
    try {
      reader.incRef();

      PrefixQuery query = new PrefixQuery(new Term(field.name(), prefixValue));
      return search(query);
    } finally {
      reader.decRef();
    }
  }

  @Override
  public IRowIdSet queryRegex(Field field, String regex) throws IOException {
    try {
      reader.incRef();

      RegexpQuery query = new RegexpQuery(new Term(field.name(), regex));
      return search(query);
    } finally {
      reader.decRef();
    }
  }

  @Override
  public IRowIdSet queryRange(
      Field field, Object min, Object max, boolean includeMin, boolean includeMax) throws IOException {
    switch (field.type().typeId()) {
      case INT:
        return queryRangeInt(field, (Integer) min, (Integer) max, includeMin, includeMax);
      case LONG:
        return queryRangeLong(field, (Long) min, (Long) max, includeMin, includeMax);
      case FLOAT:
        return queryRangeFloat(field, (Float) min, (Float) max, includeMin, includeMax);
      case DOUBLE:
        return queryRangeDouble(field, (Double) min, (Double) max, includeMin, includeMax);
      case DECIMAL:
        return queryRangeDecimal(field, (BigDecimal) min, (BigDecimal) max, includeMin, includeMax);
      case STRING:
        return queryRangeText(field, (String) min, (String) max, includeMin, includeMax);
      default:
        throw new HoodieNotSupportedException("Unsupported type for range query: " + field.type().typeId().getName());
    }
  }

  private IRowIdSet queryRangeInt(
      Field field, Integer min, Integer max, boolean includeMin, boolean includeMax) throws IOException {
    try {
      reader.incRef();

      int minBound = min == null ? Integer.MIN_VALUE : min;
      int maxBound = max == null ? Integer.MAX_VALUE : max;
      if (!includeMin) {
        minBound = Math.addExact(minBound, 1);
      }
      if (!includeMax) {
        maxBound = Math.addExact(maxBound, -1);
      }

      Query query = IntPoint.newRangeQuery(field.name(), minBound, maxBound);
      return search(query);
    } finally {
      reader.decRef();
    }
  }

  private IRowIdSet queryRangeLong(
      Field field, Long min, Long max, boolean includeMin, boolean includeMax) throws IOException {
    try {
      reader.incRef();

      long minBound = min == null ? Long.MIN_VALUE : min;
      long maxBound = max == null ? Long.MAX_VALUE : max;
      if (!includeMin) {
        minBound = Math.addExact(minBound, 1);
      }
      if (!includeMax) {
        maxBound = Math.addExact(maxBound, -1);
      }

      Query query = LongPoint.newRangeQuery(field.name(), minBound, maxBound);
      return search(query);
    } finally {
      reader.decRef();
    }
  }

  private IRowIdSet queryRangeFloat(
      Field field, Float min, Float max, boolean includeMin, boolean includeMax) throws IOException {
    try {
      reader.incRef();
      float minBound = min == null ? Float.NEGATIVE_INFINITY : min;
      float maxBound = max == null ? Float.POSITIVE_INFINITY : max;
      if (!includeMin) {
        minBound = FloatPoint.nextUp(minBound);
      }
      if (!includeMax) {
        maxBound = FloatPoint.nextDown(maxBound);
      }

      Query query = FloatPoint.newRangeQuery(field.name(), minBound, maxBound);
      return search(query);
    } finally {
      reader.decRef();
    }
  }

  private IRowIdSet queryRangeDouble(
      Field field, Double min, Double max, boolean includeMin, boolean includeMax) throws IOException {
    try {
      reader.incRef();

      double minBound = min == null ? Double.NEGATIVE_INFINITY : min;
      double maxBound = max == null ? Double.POSITIVE_INFINITY : max;
      if (!includeMin) {
        minBound = DoublePoint.nextUp(minBound);
      }
      if (!includeMax) {
        maxBound = DoublePoint.nextDown(maxBound);
      }

      Query query = DoublePoint.newRangeQuery(field.name(), minBound, maxBound);
      return search(query);
    } finally {
      reader.decRef();
    }

  }

  private IRowIdSet queryRangeDecimal(
      Field field, BigDecimal min, BigDecimal max, boolean includeMin, boolean includeMax) throws IOException {
    throw new HoodieNotSupportedException("Not supported for decimal range query");
  }

  private IRowIdSet queryRangeText(
      Field field, String min, String max, boolean includeMin, boolean includeMax) throws IOException {
    try {
      reader.incRef();

      TermRangeQuery query = TermRangeQuery.newStringRange(field.name(), min, max, includeMin, includeMax);
      return search(query);
    } finally {
      reader.decRef();
    }
  }

  private IRowIdSet search(Query query) throws IOException {
    IRowIdSet rowIdSet = new RowIdSet();
    searcher.search(query, new LuceneRowIdCollector(rowIdSet));
    return rowIdSet;
  }

  private Query buildTermQuery(Field field, Object value) {
    Query query = null;
    switch (field.type().typeId()) {
      case STRING:
        query = new TermQuery(new Term(field.name(), (String) value));
        break;
      case BINARY:
      case UUID:
        byte[] bytes = ((Utf8) value).getBytes();
        query = new TermQuery(new Term(field.name(), new String(bytes)));
        break;
      case BOOLEAN:
      case INT:
        query = IntPoint.newExactQuery(field.name(), (Integer) value);
        break;
      case LONG:
      case DATE:
      case TIME:
      case TIMESTAMP:
        query = LongPoint.newExactQuery(field.name(), (Long) value);
        break;
      case FLOAT:
        query = FloatPoint.newExactQuery(field.name(), (Float) value);
        break;
      case DOUBLE:
        query = DoublePoint.newExactQuery(field.name(), (Double) value);
        break;
      case DECIMAL:
      default:
        throw new HoodieSecondaryIndexException("Unsupported field type: " + field.type().typeId().getName());
    }

    return query;
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
