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

package org.apache.hudi.common.util;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * A utility class for future operation.
 */
public class FutureUtils {

  /**
   * Parallel CompletableFutures
   *
   * @param futures CompletableFuture list
   * @return a new CompletableFuture which will completed when all of the given CompletableFutures complete.
   */
  public static <T> CompletableFuture<List<T>> allOf(@Nonnull List<CompletableFuture<T>> futures) {
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .thenApply(aVoid ->
            futures.stream()
                // NOTE: This join wouldn't block, since all the
                //       futures are completed at this point.
                .map(CompletableFuture::join)
                .collect(Collectors.toList()));
  }
}
