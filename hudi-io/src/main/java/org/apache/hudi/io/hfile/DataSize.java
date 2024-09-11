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

package org.apache.hudi.io.hfile;

/**
 * Sizes of different primitive data structures used by HFile.
 */
public class DataSize {
  // Size of boolean in bytes
  public static final int SIZEOF_BOOLEAN = 1;

  // Size of byte in bytes
  public static final int SIZEOF_BYTE = 1;

  // Size of int (int32) in bytes
  public static final int SIZEOF_INT32 = 4;

  // Size of short (int16) in bytes
  public static final int SIZEOF_INT16 = 2;

  // Size of long (int64) in bytes
  public static final int SIZEOF_INT64 = 8;

  public static final int MAGIC_LENGTH = 8;
}
