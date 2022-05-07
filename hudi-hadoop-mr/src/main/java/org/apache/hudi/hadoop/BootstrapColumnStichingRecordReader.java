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

package org.apache.hudi.hadoop;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Stitches 2 record reader returned rows and presents a concatenated view to clients.
 */
public class BootstrapColumnStichingRecordReader implements RecordReader<NullWritable, ArrayWritable> {

  private static final Logger LOG = LogManager.getLogger(BootstrapColumnStichingRecordReader.class);

  private final RecordReader<NullWritable, ArrayWritable> leftColsRecordReader;
  private final RecordReader<NullWritable, ArrayWritable> rightColsRecordReader;
  private final int numLeftColumns;
  private final ArrayWritable values;
  private final boolean validate;

  public BootstrapColumnStichingRecordReader(RecordReader<NullWritable, ArrayWritable> left,
                                             int numLeftColumns, RecordReader<NullWritable, ArrayWritable> right, int numRightColumns, boolean validate) {
    this.leftColsRecordReader = left;
    this.rightColsRecordReader = right;
    this.validate = validate;
    this.numLeftColumns = numLeftColumns;

    ArrayWritable rightW = rightColsRecordReader.createValue();
    int numColumns = numLeftColumns + numRightColumns;
    if (rightW.getValueClass() != null) {
      values = new ArrayWritable(rightW.getValueClass(), new Writable[numColumns]);
    } else {
      String[] vals = IntStream.range(0, numColumns).mapToObj(idx -> "").collect(Collectors.toList())
          .toArray(new String[0]);
      values = new ArrayWritable(vals);
    }
    LOG.info("Total ArrayWritable Length :" + values.get().length);
  }

  @Override
  public boolean next(NullWritable key, ArrayWritable value) throws IOException {
    ArrayWritable left = leftColsRecordReader.createValue();
    ArrayWritable right = rightColsRecordReader.createValue();

    boolean hasMoreOnLeft = leftColsRecordReader.next(leftColsRecordReader.createKey(), left);
    boolean hasMoreOnRight = rightColsRecordReader.next(rightColsRecordReader.createKey(), right);
    if (validate) {
      ValidationUtils.checkArgument(hasMoreOnLeft == hasMoreOnRight,
          String.format("hasMoreOnLeft:%s, hasMoreOnRight: %s", hasMoreOnLeft, hasMoreOnRight));
    }
    for (int i = 0; i < numLeftColumns; i++) {
      value.get()[i] = left.get()[i];
    }
    for (int j = numLeftColumns; j < right.get().length; j++) {
      value.get()[j] = right.get()[j];
    }
    return hasMoreOnLeft && hasMoreOnRight;
  }

  @Override
  public NullWritable createKey() {
    return leftColsRecordReader.createKey();
  }

  @Override
  public ArrayWritable createValue() {
    return values;
  }

  @Override
  public long getPos() throws IOException {
    return leftColsRecordReader.getPos();
  }

  @Override
  public void close() throws IOException {
    leftColsRecordReader.close();
    rightColsRecordReader.close();
  }

  @Override
  public float getProgress() throws IOException {
    return leftColsRecordReader.getProgress();
  }
}
