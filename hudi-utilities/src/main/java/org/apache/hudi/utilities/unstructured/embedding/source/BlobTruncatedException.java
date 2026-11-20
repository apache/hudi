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

package org.apache.hudi.utilities.unstructured.embedding.source;

import java.io.EOFException;

/**
 * A blob's stored bytes end before the range its row records: thrown only by {@link BlobBytesSource}'s own range checks (a stream
 * that ends inside its range, or an object shorter than the recorded offset), so it says something about the stored object, the
 * same on every read: the file's bad input. A bare {@link EOFException} from the storage client is
 * not one: Hadoop also reports a dropped connection (e.g. to an HDFS NameNode) as an EOFException, which is a storage failure.
 */
public class BlobTruncatedException extends EOFException {

  private static final long serialVersionUID = 1L;

  public BlobTruncatedException(String message) {
    super(message);
  }
}
