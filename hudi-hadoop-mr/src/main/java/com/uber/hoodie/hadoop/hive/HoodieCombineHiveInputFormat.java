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

package com.uber.hoodie.hadoop.hive;

import com.uber.hoodie.hadoop.HoodieInputFormat;
import com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;

public class HoodieCombineHiveInputFormat<K extends WritableComparable, V extends Writable>
    extends org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat<K, V> {

  @Override
  protected String getParquetInputFormatClassName() {
    return HoodieInputFormat.class.getName();
  }

  @Override
  protected String getParquetRealtimeInputFormatClassName() {
    return HoodieRealtimeInputFormat.class.getName();
  }

  @Override
  protected org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat.HoodieCombineFileInputFormatShim createInputFormatShim() {
    return new HoodieCombineFileInputFormatShim<>();
  }

  public static class HoodieCombineFileInputFormatShim<K, V>
      extends org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat.HoodieCombineFileInputFormatShim {

    @Override
    protected HoodieParquetInputFormat createParquetInputFormat() {
      return new HoodieInputFormat();
    }

    @Override
    protected HoodieParquetRealtimeInputFormat createParquetRealtimeInputFormat() {
      return new HoodieRealtimeInputFormat();
    }
  }
}
