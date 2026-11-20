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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Fixed blob row contract. A row is one version of one source object: the record key is the source path's sha with the version tag
 * ({@link #SOURCE_MODIFICATION_TIME}, {@link #SOURCE_FILE_SIZE}) read from the object's status when the row is prepared, so an
 * overwrite adds a row and the earlier version's row stays. Partitioning is the job's own choice of column.
 */
public final class BlobTableSchema {
  public static final String SOURCE_PATH_SHA = "source_path_sha";
  /** The source object's modification time in epoch ms (S3: LastModified), from the status call that prepared the row. */
  public static final String SOURCE_MODIFICATION_TIME = "source_modification_time";
  public static final String SOURCE_FILE_PATH = "source_file_path";
  public static final String SOURCE_FILE_NAME = "source_file_name";
  public static final String SOURCE_FILE_EXTENSION = "source_file_extension";
  /** The source object's size in bytes from the same status call. */
  public static final String SOURCE_FILE_SIZE = "source_file_size";
  public static final String CONTENT_TYPE = "content_type";
  public static final String BLOB = "blob";
  /**
   * The error of a row with no blob (its object was confirmed gone when the row was built), in the blob stream's quarantine and in
   * the embedding stream's side output alike.
   */
  public static final String GONE_SINCE_ANNOUNCED = "gone since announced";
  /**
   * The record key of a blob row: the source path's sha and the version tag, in this order. Keeping the source version in the
   * key is a decision (an overwrite adds a row, the earlier version's row stays); moving to an identity-only key needs a new
   * {@code hoodie.table.unstructured.schema.version} and a rebuilt table.
   */
  public static final List<String> KEY_COLUMNS = Collections.unmodifiableList(Arrays.asList(SOURCE_PATH_SHA, SOURCE_MODIFICATION_TIME,
      SOURCE_FILE_SIZE));

  private BlobTableSchema() {
  }

  /** Contract columns followed by the nullable logical BLOB value. */
  public static StructType schema() {
    return new StructType(new StructField[] {
        DataTypes.createStructField(SOURCE_PATH_SHA, DataTypes.StringType, false),
        DataTypes.createStructField(SOURCE_MODIFICATION_TIME, DataTypes.LongType, false),
        DataTypes.createStructField(SOURCE_FILE_PATH, DataTypes.StringType, false),
        DataTypes.createStructField(SOURCE_FILE_NAME, DataTypes.StringType, false),
        DataTypes.createStructField(SOURCE_FILE_EXTENSION, DataTypes.StringType, false),
        DataTypes.createStructField(SOURCE_FILE_SIZE, DataTypes.LongType, false),
        DataTypes.createStructField(CONTENT_TYPE, DataTypes.StringType, true),
        new StructField(BLOB, BlobType.dataType(), true,
            new MetadataBuilder().putString(HoodieSchema.TYPE_METADATA_FIELD, HoodieSchemaType.BLOB.name()).build())
    });
  }
}
