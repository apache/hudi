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

package org.apache.hudi.integ.testsuite.writer;

import java.io.Closeable;
import java.io.IOException;

/**
 * Implementations of {@link DeltaInputWriter} will be able to generate data.
 *
 * @param <I> Data type to be generated.
 */
public interface DeltaInputWriter<I> extends Closeable {

  /**
   * Generate any type of data.
   */
  void writeData(I iData) throws IOException;

  /**
   * Check whether more data can/should be written.
   */
  boolean canWrite();

  /**
   * Return the statistics of data written.
   */
  DeltaWriteStats getDeltaWriteStats();

  /**
   * Return a new instance of this writer.
   * @return
   * @throws IOException
   */
  DeltaInputWriter getNewWriter() throws IOException;

}