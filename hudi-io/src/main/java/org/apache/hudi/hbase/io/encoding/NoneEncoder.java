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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hudi.hbase.Cell;
import org.apache.hudi.hbase.KeyValue;
import org.apache.hudi.hbase.KeyValueUtil;
import org.apache.hudi.hbase.PrivateCellUtil;
import org.apache.hadoop.io.WritableUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class NoneEncoder {

  private DataOutputStream out;
  private HFileBlockDefaultEncodingContext encodingCtx;

  public NoneEncoder(DataOutputStream out,
                     HFileBlockDefaultEncodingContext encodingCtx) {
    this.out = out;
    this.encodingCtx = encodingCtx;
  }

  public int write(Cell cell) throws IOException {
    // We write tags seperately because though there is no tag in KV
    // if the hfilecontext says include tags we need the tags length to be
    // written
    int size = KeyValueUtil.oswrite(cell, out, false);
    // Write the additional tag into the stream
    if (encodingCtx.getHFileContext().isIncludesTags()) {
      int tagsLength = cell.getTagsLength();
      out.writeShort(tagsLength);
      if (tagsLength > 0) {
        PrivateCellUtil.writeTags(out, cell, tagsLength);
      }
      size += tagsLength + KeyValue.TAGS_LENGTH_SIZE;
    }
    if (encodingCtx.getHFileContext().isIncludesMvcc()) {
      WritableUtils.writeVLong(out, cell.getSequenceId());
      size += WritableUtils.getVIntSize(cell.getSequenceId());
    }
    return size;
  }

}
