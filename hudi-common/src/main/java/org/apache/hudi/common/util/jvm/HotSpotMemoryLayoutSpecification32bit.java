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

package org.apache.hudi.common.util.jvm;

/**
 * Implementation of {@link MemoryLayoutSpecification} based on
 * Hot Spot Memory Layout Specification on 32-bit.
 */
public class HotSpotMemoryLayoutSpecification32bit implements MemoryLayoutSpecification {
  @Override
  public int getArrayHeaderSize() {
    return 12;
  }

  @Override
  public int getObjectHeaderSize() {
    return 8;
  }

  @Override
  public int getObjectPadding() {
    return 8;
  }

  @Override
  public int getReferenceSize() {
    return 4;
  }

  @Override
  public int getSuperclassFieldPadding() {
    return 4;
  }
}
