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

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class InputSplitUtils {

  public static void writeString(String str, DataOutput out) throws IOException {
    byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  public static String readString(DataInput in) throws IOException {
    byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static void writeBoolean(Boolean valueToWrite, DataOutput out) throws IOException {
    out.writeBoolean(valueToWrite);
  }

  public static boolean readBoolean(DataInput in) throws IOException {
    return in.readBoolean();
  }

  /**
   * Return correct base-file schema based on split.
   *
   * @param split File Split
   * @param conf Configuration
   * @return
   */
  public static Schema getBaseFileSchema(FileSplit split, Configuration conf) {
    try {
      if (split instanceof BootstrapBaseFileSplit) {
        HoodieFileReader storageReader = HoodieFileReaderFactory.getFileReader(conf,
            ((BootstrapBaseFileSplit)(split)).getBootstrapFileSplit().getPath());
        return HoodieAvroUtils.addMetadataFields(storageReader.getSchema());
      }
      return HoodieRealtimeRecordReaderUtils.readSchema(conf, split.getPath());
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read footer for parquet " + split.getPath(), e);
    }
  }
}
