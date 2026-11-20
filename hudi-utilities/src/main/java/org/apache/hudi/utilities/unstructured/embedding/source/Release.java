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

package org.apache.hudi.utilities.unstructured.embedding.source;

/**
 * A release step that is itself a resource: a try-with-resources over several releases runs each whatever the ones before it
 * threw (in reverse order of declaration), the first failure thrown and the later ones suppressed into it. Over a block, a
 * release runs whatever the block threw, and its failure is suppressed into the block's.
 *
 * @param <X> the checked exception the release may throw ({@link RuntimeException} for none)
 */
@FunctionalInterface
public interface Release<X extends Exception> extends AutoCloseable {
  @Override
  void close() throws X;
}
