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

package org.apache.hudi.table.action.commit;

import java.io.Serializable;
import org.apache.hudi.common.model.HoodieRecordLocation;

/**
 * Helper class for a small file's location and its actual size on disk.
 */
public class SmallFile implements Serializable {

  public HoodieRecordLocation location;
  public long sizeBytes;

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("SmallFile {");
    sb.append("location=").append(location).append(", ");
    sb.append("sizeBytes=").append(sizeBytes);
    sb.append('}');
    return sb.toString();
  }
}