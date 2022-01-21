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

import org.apache.yetus.audience.InterfaceAudience;

import java.util.List;

/**
 * Allows creating a cell with {@link Tag}
 * An instance of this type can be acquired by using RegionCoprocessorEnvironment#getCellBuilder
 * (for prod code) and {@link RawCellBuilderFactory} (for unit tests).
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
public interface RawCellBuilder extends CellBuilder {
  @Override
  RawCellBuilder setRow(final byte[] row);
  @Override
  RawCellBuilder setRow(final byte[] row, final int rOffset, final int rLength);

  @Override
  RawCellBuilder setFamily(final byte[] family);
  @Override
  RawCellBuilder setFamily(final byte[] family, final int fOffset, final int fLength);

  @Override
  RawCellBuilder setQualifier(final byte[] qualifier);
  @Override
  RawCellBuilder setQualifier(final byte[] qualifier, final int qOffset, final int qLength);

  @Override
  RawCellBuilder setTimestamp(final long timestamp);

  @Override
  RawCellBuilder setType(final Cell.Type type);

  @Override
  RawCellBuilder setValue(final byte[] value);
  @Override
  RawCellBuilder setValue(final byte[] value, final int vOffset, final int vLength);

  RawCellBuilder setTags(final List<Tag> tags);

  @Override
  RawCell build();

  @Override
  RawCellBuilder clear();
}
