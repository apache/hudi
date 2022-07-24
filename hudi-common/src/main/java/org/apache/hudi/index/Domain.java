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

package org.apache.hudi.index;

import java.util.Objects;

public class Domain {

  private final boolean nullAllowed;

  private final ValueSet values;

  public Domain(boolean nullAllowed, ValueSet values) {
    this.nullAllowed = nullAllowed;
    this.values = Objects.requireNonNull(values, "values is null");
  }

  public ValueSet getValues() {
    return values;
  }

  public boolean isNullAllowed() {
    return nullAllowed;
  }

  public boolean isNone() {
    return values.isNone() && !nullAllowed;
  }

  public boolean isAll() {
    return values.isAll() && nullAllowed;
  }

  public static Domain all(Type type) {
    return new Domain(true, ValueSet.all(type));
  }
}
