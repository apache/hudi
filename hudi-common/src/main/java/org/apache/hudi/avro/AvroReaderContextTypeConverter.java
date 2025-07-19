/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.avro;

import org.apache.hudi.common.engine.ReaderContextTypeConverter;
import org.apache.hudi.common.util.StringUtils;

import java.nio.ByteBuffer;

public class AvroReaderContextTypeConverter extends ReaderContextTypeConverter {
  @Override
  public String castToString(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof ByteBuffer) {
      ByteBuffer buffer = (ByteBuffer) value;
      return StringUtils.fromUTF8Bytes(buffer.array());
    }
    return value.toString();
  }
}
