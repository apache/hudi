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

import org.apache.hudi.table.format.cow.vector.HeapArrayGroupColumnVector;

import org.apache.flink.formats.parquet.vector.reader.ColumnReader;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;

import java.io.IOException;

/**
 * Array of a Group type (Array, Map, Row, etc.) {@link ColumnReader}.
 */
public class ArrayGroupReader implements ColumnReader<WritableColumnVector> {

  private final ColumnReader<WritableColumnVector> fieldReader;

  public ArrayGroupReader(ColumnReader<WritableColumnVector> fieldReader) {
    this.fieldReader = fieldReader;
  }

  @Override
  public void readToVector(int readNumber, WritableColumnVector vector) throws IOException {
    HeapArrayGroupColumnVector rowColumnVector = (HeapArrayGroupColumnVector) vector;

    fieldReader.readToVector(readNumber, rowColumnVector.vector);
  }
}
