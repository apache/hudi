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

package org.apache.hudi.utilities.sources.blob;

/**
 * An invalid inline heap-fraction setting, with the message naming the key and its required range. It is checked on the driver
 * before fetching events and when a blob row builder is created.
 *
 * <p>Other source configuration checks in {@link BlobSourceSetup#validate} and {@link BlobSourceSetup#check} use their own
 * exception types. This exception remains an {@link IllegalArgumentException} so callers can catch configuration errors
 * together.
 */
public class BlobSourceConfigException extends IllegalArgumentException {

  public BlobSourceConfigException(String message) {
    super(message);
  }

  public BlobSourceConfigException(String message, Throwable cause) {
    super(message, cause);
  }
}
