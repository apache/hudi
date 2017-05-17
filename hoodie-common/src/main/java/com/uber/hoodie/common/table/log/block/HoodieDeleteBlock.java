/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.table.log.block;

import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.commons.lang3.StringUtils;

/**
 * Delete block contains a list of keys to be deleted from scanning the blocks so far
 */
public class HoodieDeleteBlock implements HoodieLogBlock {

  private final String[] keysToDelete;

  public HoodieDeleteBlock(String[] keysToDelete) {
    this.keysToDelete = keysToDelete;
  }

  @Override
  public byte[] getBytes() throws IOException {
    return StringUtils.join(keysToDelete, ',').getBytes(Charset.forName("utf-8"));
  }

  public String[] getKeysToDelete() {
    return keysToDelete;
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.DELETE_BLOCK;
  }

  public static HoodieLogBlock fromBytes(byte[] content) {
    return new HoodieDeleteBlock(new String(content).split(","));
  }
}
