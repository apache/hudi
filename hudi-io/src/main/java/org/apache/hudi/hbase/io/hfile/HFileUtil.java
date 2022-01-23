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

package org.apache.hudi.hbase.io.hfile;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
class HFileUtil {

  /** guards against NullPointer
   * utility which tries to seek on the DFSIS and will try an alternative source
   * if the FSDataInputStream throws an NPE HBASE-17501
   * @param istream
   * @param offset
   * @throws IOException
   */
  static public void seekOnMultipleSources(FSDataInputStream istream, long offset) throws IOException {
    try {
      // attempt to seek inside of current blockReader
      istream.seek(offset);
    } catch (NullPointerException e) {
      // retry the seek on an alternate copy of the data
      // this can occur if the blockReader on the DFSInputStream is null
      istream.seekToNewSource(offset);
    }
  }
}
