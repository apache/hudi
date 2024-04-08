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

package org.apache.hudi.io;

import java.io.IOException;

/**
 * Represents an operation that accepts four input arguments and returns no result.
 * Unlike most other functional interfaces, {@code Consumer4} is expected to operate via side-effects.
 */
public interface Consumer4<T1, T2, T3, T4> {

  /**
   * Performs this operation on the given arguments.
   *
   * @param var1 the first argument
   * @param var2 the second argument
   * @param var3 the third argument
   * @param var4 the forth argument
   * @throws IOException
   */
  void accept(T1 var1, T2 var2, T3 var3, T4 var4) throws IOException;
}
