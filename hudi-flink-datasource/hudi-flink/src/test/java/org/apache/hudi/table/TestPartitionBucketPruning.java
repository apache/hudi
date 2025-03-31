/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table;

import org.apache.hudi.common.model.PartitionBucketIndexHashingConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.source.prune.PrimaryKeyPruners;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.util.SerializableSchema;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.hadoop.fs.Path;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class TestPartitionBucketPruning {

  @TempDir
  File tempFile;

  /**
   * test single primary key filtering
   * @throws Exception
   */
  @Test
  void testPartitionBucketPruningWithSinglePK() throws Exception {
    String tablePath1 = new Path(tempFile.getAbsolutePath(), "tbl1").toString();
    int bucketNumber = 10000;
    String expression = "par1|par2|par3|par4,4";
    String rule = "regex";
    Configuration conf1 = TestConfigurations.getDefaultConf(tablePath1);
    conf1.setString(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf1.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, bucketNumber);
    conf1.set(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS, expression);
    conf1.set(FlinkOptions.BUCKET_INDEX_PARTITION_RULE, rule);

    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf1);
    PartitionBucketIndexHashingConfig.saveHashingConfig(metaClient, expression, rule, bucketNumber, null);

    // test single primary key filtering
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf1);
    HoodieTableSource tableSource1 = createHoodieTableSource(conf1);
    tableSource1.applyFilters(Collections.singletonList(
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1")));

    assertThat(BucketIdentifier.getBucketId(tableSource1.getDataBucketHashing(), 4), is(1));
    List<StoragePathInfo> fileList = tableSource1.getReadFiles();
    assertThat("Files should be pruned by bucket id 1", fileList.size(), CoreMatchers.is(2));
  }

  /**
   * test multiple primary keys filtering
   * @throws Exception
   */
  @Test
  void testPartitionBucketPruningWithMultiPK() throws Exception {
    String tablePath = new Path(tempFile.getAbsolutePath(), "tbl1").toString();
    int bucketNumber = 10000;
    String expression = "par1|par2|par3|par4,4";
    String rule = "regex";
    Configuration conf = TestConfigurations.getDefaultConf(tablePath);
    conf.setString(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, bucketNumber);
    conf.set(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS, expression);
    conf.set(FlinkOptions.BUCKET_INDEX_PARTITION_RULE, rule);
    conf.setString(FlinkOptions.RECORD_KEY_FIELD, "uuid,name");
    conf.setString(FlinkOptions.KEYGEN_TYPE, "COMPLEX");

    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
    PartitionBucketIndexHashingConfig.saveHashingConfig(metaClient, expression, rule, bucketNumber, null);
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf);
    HoodieTableSource tableSource = createHoodieTableSource(conf);
    tableSource.applyFilters(Arrays.asList(
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1"),
        createLitEquivalenceExpr("name", 1, DataTypes.STRING().notNull(), "Danny")));
    assertThat(BucketIdentifier.getBucketId(tableSource.getDataBucketHashing(), 4), is(3));
    List<StoragePathInfo> fileList = tableSource.getReadFiles();
    assertThat("Files should be pruned by bucket id 3", fileList.size(), CoreMatchers.is(3));
  }

  /**
   * test partial primary keys filtering
   * @throws Exception
   */
  @Test
  void testPartialPartitionBucketPruningWithMultiPK() throws Exception {
    String tablePath = new Path(tempFile.getAbsolutePath(), "tbl1").toString();
    int bucketNumber = 10000;
    String expression = "par1|par2|par3|par4,4";
    String rule = "regex";
    Configuration conf = TestConfigurations.getDefaultConf(tablePath);
    conf.setString(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, bucketNumber);
    conf.set(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS, expression);
    conf.set(FlinkOptions.BUCKET_INDEX_PARTITION_RULE, rule);
    conf.setString(FlinkOptions.RECORD_KEY_FIELD, "uuid,name");
    conf.setString(FlinkOptions.KEYGEN_TYPE, "COMPLEX");

    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
    PartitionBucketIndexHashingConfig.saveHashingConfig(metaClient, expression, rule, bucketNumber, null);
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf);
    HoodieTableSource tableSource = createHoodieTableSource(conf);
    tableSource.applyFilters(Collections.singletonList(
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1")));

    assertThat(BucketIdentifier.getBucketId(tableSource.getDataBucketHashing(), 4), is(PrimaryKeyPruners.BUCKET_ID_NO_PRUNING));
    List<StoragePathInfo> fileList = tableSource.getReadFiles();
    assertThat("Partial pk filtering does not prune any files", fileList.size(),
        CoreMatchers.is(7));
  }

  /**
   * test single primary keys filtering together with non-primary key predicate
   * @throws Exception
   */
  @Test
  void testPartitionBucketPruningWithMixedFilter() throws Exception {
    String tablePath = new Path(tempFile.getAbsolutePath(), "tbl1").toString();
    int bucketNumber = 10000;
    String expression = "par1|par2|par3|par4,4";
    String rule = "regex";
    Configuration conf = TestConfigurations.getDefaultConf(tablePath);
    conf.setString(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, bucketNumber);
    conf.set(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS, expression);
    conf.set(FlinkOptions.BUCKET_INDEX_PARTITION_RULE, rule);

    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
    PartitionBucketIndexHashingConfig.saveHashingConfig(metaClient, expression, rule, bucketNumber, null);
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf);
    HoodieTableSource tableSource = createHoodieTableSource(conf);
    tableSource.applyFilters(Arrays.asList(
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1"),
        createLitEquivalenceExpr("name", 1, DataTypes.STRING().notNull(), "Danny")));

    assertThat(BucketIdentifier.getBucketId(tableSource.getDataBucketHashing(), 4), is(1));
    List<StoragePathInfo> fileList = tableSource.getReadFiles();
    assertThat("Files should be pruned by bucket id 1", fileList.size(), CoreMatchers.is(2));
  }

  private HoodieTableSource createHoodieTableSource(Configuration conf) {
    return new HoodieTableSource(
        SerializableSchema.create(TestConfigurations.TABLE_SCHEMA),
        new StoragePath(conf.getString(FlinkOptions.PATH)),
        Arrays.asList(conf.getString(FlinkOptions.PARTITION_PATH_FIELD).split(",")),
        "default-par",
        conf);
  }

  private ResolvedExpression createLitEquivalenceExpr(String fieldName, int fieldIdx, DataType dataType, Object val) {
    FieldReferenceExpression ref = new FieldReferenceExpression(fieldName, dataType, fieldIdx, fieldIdx);
    ValueLiteralExpression literal = new ValueLiteralExpression(val, dataType);
    return new CallExpression(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(ref, literal),
        DataTypes.BOOLEAN());
  }
}
