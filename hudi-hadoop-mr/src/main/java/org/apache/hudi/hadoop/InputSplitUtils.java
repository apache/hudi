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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

public class InputSplitUtils {

  public static void writeString(String str, DataOutput out) throws IOException {
    byte[] bytes = getUTF8Bytes(str);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  public static String readString(DataInput in) throws IOException {
    byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    return fromUTF8Bytes(bytes);
  }

  public static void writeBoolean(Boolean valueToWrite, DataOutput out) throws IOException {
    out.writeBoolean(valueToWrite);
  }

  public static boolean readBoolean(DataInput in) throws IOException {
    return in.readBoolean();
  }
}
