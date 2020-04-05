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

import org.apache.hadoop.mapred.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;


/**
 * Sub-type of File Split which encapsulates both skeleton and external base file splits.
 */
public class ExternalBaseFileSplit extends FileSplit {

  private FileSplit externalFileSplit;

  public ExternalBaseFileSplit() {
    super();
  }

  public ExternalBaseFileSplit(FileSplit baseSplit, FileSplit externalFileSplit)
      throws IOException {
    super(baseSplit.getPath(), baseSplit.getStart(), baseSplit.getLength(), baseSplit.getLocations());
    this.externalFileSplit = externalFileSplit;
  }

  public FileSplit getExternalFileSplit() {
    return externalFileSplit;
  }

  private static void writeString(String str, DataOutput out) throws IOException {
    byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  public static String readString(DataInput in) throws IOException {
    byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    externalFileSplit.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    externalFileSplit = new HFileSplit();
    externalFileSplit.readFields(in);
  }

  /**
   * Wrapper for FileSplit just to expose default constructor to the outer class.
   */
  public static class HFileSplit extends FileSplit {

    public HFileSplit() {
      super();
    }
  }
}