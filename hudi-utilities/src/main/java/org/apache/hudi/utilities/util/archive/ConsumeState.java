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

package org.apache.hudi.utilities.util.archive;

/**
 * Where an archive file sits in the elastic consumption window.
 *
 * <p>The window's invariant is that {@code CONSUMING} and {@code CONSUMED} files together are always
 * exactly a prefix of the (newest-first) file list, and everything after that prefix is
 * {@code NOT_CONSUMED}. That is what guarantees no file is ever skipped. It does <b>not</b> mean the
 * {@code CONSUMING} files themselves are contiguous -- files complete out of order, so a completed
 * file may sit between two in-flight ones.
 */
public enum ConsumeState {
  NOT_CONSUMED,
  CONSUMING,
  CONSUMED
}
