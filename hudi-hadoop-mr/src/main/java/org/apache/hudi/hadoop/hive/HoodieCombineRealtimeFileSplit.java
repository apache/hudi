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

package org.apache.hudi.hadoop.hive;

import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.ArrayUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;

import lombok.Getter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a CombineFileSplit for realtime tables.
 */
public class HoodieCombineRealtimeFileSplit extends CombineFileSplit {

  // These are instances of HoodieRealtimeSplits
  @Getter
  List<FileSplit> realtimeFileSplits = new ArrayList<>();

  public HoodieCombineRealtimeFileSplit() {
  }

  public HoodieCombineRealtimeFileSplit(JobConf jobConf, List<FileSplit> realtimeFileSplits) {
    super(jobConf, realtimeFileSplits.stream().map(p ->
            p.getPath()).collect(Collectors.toList()).toArray(new
            Path[realtimeFileSplits.size()]),
        ArrayUtils.toPrimitive(realtimeFileSplits.stream().map(p -> p.getStart())
            .collect(Collectors.toList()).toArray(new Long[realtimeFileSplits.size()])),
        ArrayUtils.toPrimitive(realtimeFileSplits.stream().map(p -> p.getLength())
            .collect(Collectors.toList()).toArray(new Long[realtimeFileSplits.size()])),
        realtimeFileSplits.stream().map(p -> {
          try {
            return Arrays.asList(p.getLocations());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }).flatMap(List::stream).collect(Collectors.toList()).toArray(new
            String[realtimeFileSplits.size()]));
    this.realtimeFileSplits = realtimeFileSplits;

  }

  @Override
  public String toString() {
    return "HoodieCombineRealtimeFileSplit{"
        + "realtimeFileSplits=" + realtimeFileSplits
        + '}';
  }

  /**
   * Writable interface.
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(realtimeFileSplits.size());
    for (InputSplit inputSplit: realtimeFileSplits) {
      Text.writeString(out, inputSplit.getClass().getName());
      inputSplit.write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    int listLength = in.readInt();
    realtimeFileSplits = new ArrayList<>(listLength);
    for (int i = 0; i < listLength; i++) {
      String inputClassName = Text.readString(in);
      HoodieRealtimeFileSplit inputSplit = ReflectionUtils.loadClass(inputClassName);
      inputSplit.readFields(in);
      realtimeFileSplits.add(inputSplit);
    }
  }

  public long getLength() {
    return realtimeFileSplits.size();
  }

  /** Returns an array containing the start offsets of the files in the split. */
  public long[] getStartOffsets() {
    return realtimeFileSplits.stream().mapToLong(x -> 0L).toArray();
  }

  /** Returns an array containing the lengths of the files in the split. */
  public long[] getLengths() {
    return realtimeFileSplits.stream().mapToLong(FileSplit::getLength).toArray();
  }

  /** Returns the start offset of the i<sup>th</sup> Path. */
  public long getOffset(int i) {
    return 0;
  }

  /** Returns the length of the i<sup>th</sup> Path. */
  public long getLength(int i) {
    return realtimeFileSplits.get(i).getLength();
  }

  /** Returns the number of Paths in the split. */
  public int getNumPaths() {
    return realtimeFileSplits.size();
  }

  /** Returns the i<sup>th</sup> Path. */
  public Path getPath(int i) {
    return realtimeFileSplits.get(i).getPath();
  }

  /** Returns all the Paths in the split. */
  public Path[] getPaths() {
    return realtimeFileSplits.stream().map(x -> x.getPath()).toArray(Path[]::new);
  }

  /** Returns all the Paths where this input-split resides. */
  public String[] getLocations() throws IOException {
    return realtimeFileSplits.stream().flatMap(x -> {
      try {
        return Arrays.stream(x.getLocations());
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    }).toArray(String[]::new);
  }

  public static class Builder {

    // These are instances of HoodieRealtimeSplits
    public List<FileSplit> fileSplits = new ArrayList<>();

    public void addSplit(FileSplit split) {
      fileSplits.add(split);
    }

    public HoodieCombineRealtimeFileSplit build(JobConf conf) {
      return new HoodieCombineRealtimeFileSplit(conf, fileSplits);
    }
  }
}
