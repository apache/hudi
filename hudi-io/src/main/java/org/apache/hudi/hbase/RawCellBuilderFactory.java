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
 * Factory for creating cells for CPs. It does deep_copy {@link CellBuilderType#DEEP_COPY} while
 * creating cells.
 * This class is limited private only for use in unit-tests.
 * For non-test uses in coprocessors, get an instance of type {@link RawCellBuilder}
 * using RegionCoprocessorEnvironment#getCellBuilder.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.UNITTEST)
public final class RawCellBuilderFactory {

  /**
   * @return the cell that is created
   */
  public static RawCellBuilder create() {
    return new KeyValueBuilder();
  }

  private RawCellBuilderFactory() {
  }
}
