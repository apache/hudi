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
 * Create a CellBuilder instance. Currently, we have two kinds of Cell Builder.
 * {@link CellBuilderType#DEEP_COPY} All bytes array passed into builder will be copied to build an new Cell.
 *                                   The cell impl is {@link org.apache.hudi.hbase.KeyValue}
 * {@link CellBuilderType#SHALLOW_COPY} Just copy the references of passed bytes array to build an new Cell
 *                                      The cell impl is {@link org.apache.hudi.hbase.IndividualBytesFieldCell}
 * NOTE: The cell impl may be changed in the future. The user application SHOULD NOT depend on any concrete cell impl.
 */
@InterfaceAudience.Public
public final class CellBuilderFactory {

  /**
   * Create a CellBuilder instance.
   * @param type indicates which memory copy is used in building cell.
   * @return An new CellBuilder
   */
  public static CellBuilder create(CellBuilderType type) {
    switch (type) {
      case SHALLOW_COPY:
        return new IndividualBytesFieldCellBuilder();
      case DEEP_COPY:
        return new KeyValueBuilder();
      default:
        throw new UnsupportedOperationException("The type:" + type + " is unsupported");
    }
  }

  private CellBuilderFactory(){
  }
}
