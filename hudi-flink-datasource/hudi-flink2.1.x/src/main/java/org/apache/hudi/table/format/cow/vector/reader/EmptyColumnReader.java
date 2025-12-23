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

package org.apache.hudi.table.format.cow.vector.reader;

import org.apache.flink.formats.parquet.vector.reader.ColumnReader;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;

import java.io.IOException;

/**
 * Empty {@link ColumnReader}.
 * <p>
 * This reader is to handle parquet files that have not been updated to the latest Schema.
 * When reading a parquet file with the latest schema, parquet file might not have the new field.
 * The EmptyColumnReader is used to handle such scenarios.
 */
public class EmptyColumnReader implements ColumnReader<WritableColumnVector> {

  public EmptyColumnReader() {
  }

  @Override
  public void readToVector(int readNumber, WritableColumnVector vector) throws IOException {
    vector.fillWithNulls();
  }
}

