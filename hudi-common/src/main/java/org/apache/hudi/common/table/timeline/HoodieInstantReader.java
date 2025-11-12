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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import java.io.IOException;
import java.io.InputStream;

/**
 * Interface for classes that are capable of reading the contents of a HoodieInstant.
 */
public interface HoodieInstantReader {
  /**
   * Reads the provided instant's content into a stream for parsing.
   *
   * @param instant the instant to read
   * @return an InputStream with the content
   */
  default InputStream getContentStream(HoodieInstant instant) {
    throw new RuntimeException("Not implemented");
  }

  /**
   * Reads the provided instant's content into a byte array for parsing.
   * @param instant the instant to read
   * @return an InputStream with the details
   */
  @Deprecated
  default Option<byte[]> getInstantDetails(HoodieInstant instant) {
    try (InputStream inputStream = getContentStream(instant)) {
      return Option.of(FileIOUtils.readAsByteArray(inputStream));
    } catch (IOException ex) {
      throw new HoodieIOException("Could not read commit details from stream", ex);
    }
  }
}
