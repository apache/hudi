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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The blob table's record key through the two key generators a job may run: {@link ComplexKeyGenerator} for a partitioned table
 * and {@link NonpartitionedKeyGenerator} for an unpartitioned one both combine the three key columns into the same
 * {@code field:value,...} string, through the Row path the quarantine transformer uses and the Avro path the writer uses.
 */
public class TestVersionRecordKeyGenerators {

  private static final String KEY_FIELDS = BlobTableSchema.SOURCE_PATH_SHA + "," + BlobTableSchema.SOURCE_MODIFICATION_TIME + ","
      + BlobTableSchema.SOURCE_FILE_SIZE;
  private static final String EXPECTED_KEY = "source_path_sha:3f9a,source_modification_time:1761811200000,source_file_size:52431";

  private static TypedProperties props(String partitionField) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.recordkey.field", KEY_FIELDS);
    props.setProperty("hoodie.datasource.write.partitionpath.field", partitionField);
    return props;
  }

  private static Row row() {
    return new GenericRowWithSchema(new Object[] {"3f9a", 1761811200000L, "s3://b/a.pdf", "a.pdf", "pdf", 52431L, "application/pdf", null},
        BlobTableSchema.schema());
  }

  private static GenericRecord record() {
    Schema schema = new Schema.Parser().parse(
        HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(BlobTableSchema.schema(), "blob_record", "hoodie.blob").toString());
    GenericRecord record = new GenericData.Record(schema);
    record.put(BlobTableSchema.SOURCE_PATH_SHA, "3f9a");
    record.put(BlobTableSchema.SOURCE_MODIFICATION_TIME, 1761811200000L);
    record.put(BlobTableSchema.SOURCE_FILE_PATH, "s3://b/a.pdf");
    record.put(BlobTableSchema.SOURCE_FILE_NAME, "a.pdf");
    record.put(BlobTableSchema.SOURCE_FILE_EXTENSION, "pdf");
    record.put(BlobTableSchema.SOURCE_FILE_SIZE, 52431L);
    record.put(BlobTableSchema.CONTENT_TYPE, "application/pdf");
    record.put(BlobTableSchema.BLOB, null);
    return record;
  }

  @Test
  public void complexAndNonpartitionedGeneratorsBuildTheSameRecordKey() {
    ComplexKeyGenerator partitioned = new ComplexKeyGenerator(props(BlobTableSchema.SOURCE_FILE_EXTENSION));
    ComplexKeyGenerator complexUnpartitioned = new ComplexKeyGenerator(props(""));
    NonpartitionedKeyGenerator unpartitioned = new NonpartitionedKeyGenerator(props(""));
    assertEquals(EXPECTED_KEY, partitioned.getRecordKey(row()));
    assertEquals(EXPECTED_KEY, complexUnpartitioned.getRecordKey(row()));
    assertEquals(EXPECTED_KEY, unpartitioned.getRecordKey(row()));
    assertEquals("pdf", partitioned.getPartitionPath(row()));
    assertEquals("", complexUnpartitioned.getPartitionPath(row()));
    assertEquals("", unpartitioned.getPartitionPath(row()));
  }

  @Test
  public void theAvroPathBuildsTheSameRecordKey() {
    assertEquals(EXPECTED_KEY, new ComplexKeyGenerator(props(BlobTableSchema.SOURCE_FILE_EXTENSION)).getRecordKey(record()));
    assertEquals(EXPECTED_KEY, new ComplexKeyGenerator(props("")).getRecordKey(record()));
    assertEquals(EXPECTED_KEY, new NonpartitionedKeyGenerator(props("")).getRecordKey(record()));
    assertEquals("pdf", new ComplexKeyGenerator(props(BlobTableSchema.SOURCE_FILE_EXTENSION)).getPartitionPath(record()));
  }

  /** What a job gets when it names no key generator class: stock inference. */
  @Test
  public void stockInferencePicksNonPartitionedWithoutAPartitionFieldAndComplexWithOne() {
    assertEquals(KeyGeneratorType.NON_PARTITION, KeyGenUtils.inferKeyGeneratorType(Option.of(KEY_FIELDS), ""));
    assertEquals(KeyGeneratorType.COMPLEX, KeyGenUtils.inferKeyGeneratorType(Option.of(KEY_FIELDS), BlobTableSchema.SOURCE_FILE_EXTENSION));
  }
}
