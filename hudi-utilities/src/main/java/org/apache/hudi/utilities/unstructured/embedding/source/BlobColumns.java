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

package org.apache.hudi.utilities.unstructured.embedding.source;

import org.apache.hudi.utilities.sources.blob.BlobTableSchema;

/**
 * Column names of the source rows the embedding job reads: the rows of the unstructured data source, in the blob row
 * shape {@link BlobTableSchema} declares. Only the columns the embedding side reads are listed; this class refers to them.
 */
public final class BlobColumns {

  public static final String SOURCE_PATH_SHA = BlobTableSchema.SOURCE_PATH_SHA;
  /** The source object's modification time (ms), half of the version tag every embedding row of the file carries. */
  public static final String SOURCE_MODIFICATION_TIME = BlobTableSchema.SOURCE_MODIFICATION_TIME;
  public static final String SOURCE_FILE_PATH = BlobTableSchema.SOURCE_FILE_PATH;
  public static final String SOURCE_FILE_NAME = BlobTableSchema.SOURCE_FILE_NAME;
  public static final String SOURCE_FILE_EXTENSION = BlobTableSchema.SOURCE_FILE_EXTENSION;
  /** The source object's size, the other half of the version tag. */
  public static final String SOURCE_FILE_SIZE = BlobTableSchema.SOURCE_FILE_SIZE;
  public static final String CONTENT_TYPE = BlobTableSchema.CONTENT_TYPE;
  public static final String BLOB = BlobTableSchema.BLOB;

  private BlobColumns() {
  }
}
