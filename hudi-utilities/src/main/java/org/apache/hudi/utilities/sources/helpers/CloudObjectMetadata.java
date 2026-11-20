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

package org.apache.hudi.utilities.sources.helpers;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class CloudObjectMetadata implements Serializable {

  public static final long UNKNOWN_MODIFICATION_TIME = 0L;

  private final String path;
  private final long size;
  /**
   * Epoch millis at which the notification reported the object was written, or
   * {@link #UNKNOWN_MODIFICATION_TIME} when the events carry no usable timestamp. Readers that
   * need a timestamp must fall back to interrogating the object when this is unknown.
   */
  private final long modificationTime;

  public CloudObjectMetadata(String path, long size) {
    this(path, size, UNKNOWN_MODIFICATION_TIME);
  }

  public CloudObjectMetadata(String path, long size, long modificationTime) {
    this.path = path;
    this.size = size;
    this.modificationTime = modificationTime;
  }
}
