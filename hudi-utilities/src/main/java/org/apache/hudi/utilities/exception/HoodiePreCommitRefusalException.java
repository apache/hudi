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

package org.apache.hudi.utilities.exception;

import org.apache.hudi.exception.HoodieValidationException;

/**
 * A refusal raised by {@link org.apache.hudi.utilities.transform.Transformer#onPreCommit} inside the data-table commit
 * transaction: the round must not commit. The streamer rolls the refused instant back before it rethrows this exception, as it
 * does for its other pre-commit refusals.
 */
public class HoodiePreCommitRefusalException extends HoodieValidationException {

  public HoodiePreCommitRefusalException(String msg) {
    super(msg);
  }

  public HoodiePreCommitRefusalException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
