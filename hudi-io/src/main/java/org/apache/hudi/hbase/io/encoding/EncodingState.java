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

package org.apache.hudi.hbase.io.encoding;

import org.apache.hudi.hbase.Cell;
import org.apache.hudi.hbase.KeyValueUtil;
import org.apache.yetus.audience.InterfaceAudience;
/**
 * Keeps track of the encoding state.
 */
@InterfaceAudience.Private
public class EncodingState {

  /**
   * The previous Cell the encoder encoded.
   */
  protected Cell prevCell = null;

  // Size of actual data being written. Not considering the block encoding/compression. This
  // includes the header size also.
  protected int unencodedDataSizeWritten = 0;

  // Size of actual data being written. considering the block encoding. This
  // includes the header size also.
  protected int encodedDataSizeWritten = 0;

  public void beforeShipped() {
    if (this.prevCell != null) {
      // can't use KeyValueUtil#toNewKeyCell, because we need both key and value
      // from the prevCell in FastDiffDeltaEncoder
      this.prevCell = KeyValueUtil.copyToNewKeyValue(this.prevCell);
    }
  }

  public void postCellEncode(int unencodedCellSizeWritten, int encodedCellSizeWritten) {
    this.unencodedDataSizeWritten += unencodedCellSizeWritten;
    this.encodedDataSizeWritten += encodedCellSizeWritten;
  }

  public int getUnencodedDataSizeWritten() {
    return unencodedDataSizeWritten;
  }

  public int getEncodedDataSizeWritten() {
    return encodedDataSizeWritten;
  }
}
