/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.applied.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.applied.common.AppliedUtils;
import org.apache.hudi.utilities.applied.common.UrsaBatchInfo;
import org.apache.hudi.utilities.applied.common.UrsaS3FilesFromBatchRecord;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.S3EventsSource;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

public class UrsaBatchS3EventsSource extends S3EventsSource {
  private static final Logger LOG = LoggerFactory.getLogger(UrsaBatchS3EventsSource.class);

  public UrsaBatchS3EventsSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession, SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  protected static class BatchRecordGenFunction implements FlatMapFunction<Row, UrsaS3FilesFromBatchRecord> {
    private S3Client s3;
    private S3Utilities s3Utilities;

    protected UrsaBatchInfo getBatchInfo(String region, String bucket, String key) {
      this.s3 = S3Client.builder().region(Region.of(region)).build();
      this.s3Utilities = s3.utilities();
      try {
        byte[] data = AppliedUtils.getS3ObjectAsBytes(s3, bucket, key);
        String dataStr = new String(data);
        return UrsaBatchInfo.fromJsonString(dataStr);
      } catch (S3Exception e) {
        throw new HoodieException("Error reading batch file from S3", e);
      } catch (Exception e) {
        throw new HoodieException("Error reading batch file", e);
      }
    }

    protected S3Uri getS3Uri(String file) {
      try {
        return s3Utilities.parseUri(new URI(file));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Iterator<UrsaS3FilesFromBatchRecord> call(Row row) throws Exception {
      // Fetch Batch file and extract files.
      String region = row.getAs("awsRegion");
      String bucketName = row.getAs("name");
      String key = row.getAs("key");
      LOG.info("Reading batch file from S3 Bucket: " + bucketName + ", Key: " + key);
      UrsaBatchInfo batchInfo = getBatchInfo(region, bucketName, key);
      if (batchInfo != null) {
        return batchInfo.getFiles().stream()
                .map(file -> UrsaS3FilesFromBatchRecord.fromBatchInfo(bucketName, key, batchInfo,
                        getS3Uri(file), region)).iterator();
      }
      return Collections.<UrsaS3FilesFromBatchRecord>emptyIterator();
    }
  }

  protected static class FileValidatorFunction implements MapFunction<UrsaS3FilesFromBatchRecord, UrsaS3FilesFromBatchRecord> {

    protected Pair<Long, Option<String>> getS3FileLength(String region, String bucket, String key) {
      S3Client client = S3Client.builder().region(Region.of(region)).build();
      try {
        HeadObjectResponse response = client.headObject(builder -> builder.bucket(bucket).key(key));
        return Pair.of(response.contentLength(), Option.empty());
      } catch (S3Exception e) {
        LOG.error("Error reading S3 file: " + key + " from bucket: " + bucket, e);
        return Pair.of(-1L, Option.of(e.getCause().toString()));
      }
    }

    @Override
    public UrsaS3FilesFromBatchRecord call(UrsaS3FilesFromBatchRecord ursaS3FilesFromBatchRecord) throws Exception {
      // Do HEAD request to get the content length and check validity.
      Pair<Long, Option<String>> details = getS3FileLength(ursaS3FilesFromBatchRecord.getBatchBucketRegion(),
              ursaS3FilesFromBatchRecord.getS3().getBucket().getName(), ursaS3FilesFromBatchRecord.getS3().getObject().getKey());
      ursaS3FilesFromBatchRecord.setValid(details.getKey() > 0);
      ursaS3FilesFromBatchRecord.getS3().getObject().setSize(details.getKey());
      details.getValue().ifPresent(awsErrorDetails -> {
        ursaS3FilesFromBatchRecord.getS3().getObject().setError(awsErrorDetails);
      });
      return ursaS3FilesFromBatchRecord;
    }
  }

  protected Dataset<Row> fetchParquetFilesFromBatches(Dataset<Row> dataset, BatchRecordGenFunction batchRecordGenFn,
                                                      FileValidatorFunction validatorFn) {
    Dataset<Row> files = dataset.flatMap(batchRecordGenFn,Encoders.bean(UrsaS3FilesFromBatchRecord.class))
            .map(validatorFn, Encoders.bean(UrsaS3FilesFromBatchRecord.class)).toDF();
    for (String col: files.columns()) {
      String renamedCol = UrsaS3FilesFromBatchRecord.FIELD_TO_COLUMN_MAP.get(col);
      if (!col.equals(renamedCol)) {
        files = files.withColumnRenamed(col, UrsaS3FilesFromBatchRecord.FIELD_TO_COLUMN_MAP.get(col));
      }
    }
    files = files.select(Arrays.stream(UrsaS3FilesFromBatchRecord.ORDERED_COLUMNS_IN_FILES_TABLE).map(Column::new).toArray(Column[]::new));
    return files;
  }

  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    Pair<Option<Dataset<Row>>, String> request = super.fetchNextBatch(lastCkptStr, sourceLimit);
    if (request.getKey().isPresent()) {
      Dataset<Row> dataset = request.getKey().get().select("awsRegion", "s3.bucket.name", "s3.object.key");
      Dataset<Row> files = fetchParquetFilesFromBatches(dataset, new BatchRecordGenFunction(), new FileValidatorFunction());
      return Pair.of(Option.of(files), request.getValue());
    }
    return request;
  }
}