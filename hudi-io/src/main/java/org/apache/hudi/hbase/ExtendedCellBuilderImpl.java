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

package org.apache.hudi.hbase;

import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public abstract class ExtendedCellBuilderImpl implements ExtendedCellBuilder {
  protected byte[] row = null;
  protected int rOffset = 0;
  protected int rLength = 0;
  protected byte[] family = null;
  protected int fOffset = 0;
  protected int fLength = 0;
  protected byte[] qualifier = null;
  protected int qOffset = 0;
  protected int qLength = 0;
  protected long timestamp = HConstants.LATEST_TIMESTAMP;
  protected KeyValue.Type type = null;
  protected byte[] value = null;
  protected int vOffset = 0;
  protected int vLength = 0;
  protected long seqId = 0;
  protected byte[] tags = null;
  protected int tagsOffset = 0;
  protected int tagsLength = 0;

  @Override
  public ExtendedCellBuilder setRow(final byte[] row) {
    return setRow(row, 0, ArrayUtils.getLength(row));
  }

  @Override
  public ExtendedCellBuilder setRow(final byte[] row, int rOffset, int rLength) {
    this.row = row;
    this.rOffset = rOffset;
    this.rLength = rLength;
    return this;
  }

  @Override
  public ExtendedCellBuilder setFamily(final byte[] family) {
    return setFamily(family, 0, ArrayUtils.getLength(family));
  }

  @Override
  public ExtendedCellBuilder setFamily(final byte[] family, int fOffset, int fLength) {
    this.family = family;
    this.fOffset = fOffset;
    this.fLength = fLength;
    return this;
  }

  @Override
  public ExtendedCellBuilder setQualifier(final byte[] qualifier) {
    return setQualifier(qualifier, 0, ArrayUtils.getLength(qualifier));
  }

  @Override
  public ExtendedCellBuilder setQualifier(final byte[] qualifier, int qOffset, int qLength) {
    this.qualifier = qualifier;
    this.qOffset = qOffset;
    this.qLength = qLength;
    return this;
  }

  @Override
  public ExtendedCellBuilder setTimestamp(final long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  @Override
  public ExtendedCellBuilder setType(final Cell.Type type) {
    this.type = PrivateCellUtil.toTypeByte(type);
    return this;
  }

  @Override
  public ExtendedCellBuilder setType(final byte type) {
    this.type = KeyValue.Type.codeToType(type);
    return this;
  }

  @Override
  public ExtendedCellBuilder setValue(final byte[] value) {
    return setValue(value, 0, ArrayUtils.getLength(value));
  }

  @Override
  public ExtendedCellBuilder setValue(final byte[] value, int vOffset, int vLength) {
    this.value = value;
    this.vOffset = vOffset;
    this.vLength = vLength;
    return this;
  }

  @Override
  public ExtendedCellBuilder setTags(final byte[] tags) {
    return setTags(tags, 0, ArrayUtils.getLength(tags));
  }

  @Override
  public ExtendedCellBuilder setTags(final byte[] tags, int tagsOffset, int tagsLength) {
    this.tags = tags;
    this.tagsOffset = tagsOffset;
    this.tagsLength = tagsLength;
    return this;
  }

  @Override
  public ExtendedCellBuilder setTags(List<Tag> tags) {
    byte[] tagBytes = TagUtil.fromList(tags);
    return setTags(tagBytes);
  }

  @Override
  public ExtendedCellBuilder setSequenceId(final long seqId) {
    this.seqId = seqId;
    return this;
  }

  private void checkBeforeBuild() {
    if (type == null) {
      throw new IllegalArgumentException("The type can't be NULL");
    }
  }

  protected abstract ExtendedCell innerBuild();

  @Override
  public ExtendedCell build() {
    checkBeforeBuild();
    return innerBuild();
  }

  @Override
  public ExtendedCellBuilder clear() {
    row = null;
    rOffset = 0;
    rLength = 0;
    family = null;
    fOffset = 0;
    fLength = 0;
    qualifier = null;
    qOffset = 0;
    qLength = 0;
    timestamp = HConstants.LATEST_TIMESTAMP;
    type = null;
    value = null;
    vOffset = 0;
    vLength = 0;
    seqId = 0;
    tags = null;
    tagsOffset = 0;
    tagsLength = 0;
    return this;
  }
}
