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

/**
 * Use {@link CellBuilderFactory} to get CellBuilder instance.
 */
@InterfaceAudience.Public
public interface CellBuilder {

  CellBuilder setRow(final byte[] row);
  CellBuilder setRow(final byte[] row, final int rOffset, final int rLength);

  CellBuilder setFamily(final byte[] family);
  CellBuilder setFamily(final byte[] family, final int fOffset, final int fLength);

  CellBuilder setQualifier(final byte[] qualifier);
  CellBuilder setQualifier(final byte[] qualifier, final int qOffset, final int qLength);

  CellBuilder setTimestamp(final long timestamp);

  CellBuilder setType(final Cell.Type type);

  CellBuilder setValue(final byte[] value);
  CellBuilder setValue(final byte[] value, final int vOffset, final int vLength);

  Cell build();

  /**
   * Remove all internal elements from builder.
   * @return this
   */
  CellBuilder clear();
}
