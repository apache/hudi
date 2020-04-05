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
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hudi.common.util.ValidationUtils;

public class HoodieColumnStichingRecordReader implements RecordReader<NullWritable, ArrayWritable> {

  private final RecordReader<NullWritable, ArrayWritable> leftColsRecordReader;
  private final RecordReader<NullWritable, ArrayWritable> rightColsRecordReader;

  private final ArrayWritable values;

  public HoodieColumnStichingRecordReader(RecordReader<NullWritable, ArrayWritable> left,
      RecordReader<NullWritable, ArrayWritable> right) {
    this.leftColsRecordReader = left;
    this.rightColsRecordReader = right;

    ArrayWritable leftW = leftColsRecordReader.createValue();
    ArrayWritable rightW = rightColsRecordReader.createValue();
    int numColumns = leftW.get().length + rightW.get().length;
    if (rightW.getValueClass() != null) {
      values = new ArrayWritable(rightW.getValueClass(), new Writable[numColumns]);
    } else {
      String[] vals = IntStream.range(0, numColumns).mapToObj(idx -> "").collect(Collectors.toList())
          .toArray(new String[0]);
      values = new ArrayWritable(vals);
    }
  }

  @Override
  public boolean next(NullWritable key, ArrayWritable value) throws IOException {
    ArrayWritable left = leftColsRecordReader.createValue();
    ArrayWritable right = rightColsRecordReader.createValue();

    boolean hasMoreOnLeft = leftColsRecordReader.next(leftColsRecordReader.createKey(), left);
    boolean hasMoreOnRight = rightColsRecordReader.next(rightColsRecordReader.createKey(), right);
    ValidationUtils.checkArgument(hasMoreOnLeft == hasMoreOnRight);
    int i = 0;
    for (;i < left.get().length; i++) {
      value.get()[i] = left.get()[i];
    }

    for (int j = 0; j < right.get().length; j++) {
      value.get()[i++] = right.get()[j];
    }
    System.out.println("Left Record :" + Arrays.asList(left.get()));
    System.out.println("Right Record :" + Arrays.asList(right.get()));
    System.out.println("Stiched Record :" + Arrays.asList(value.get()));
    return hasMoreOnLeft;
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