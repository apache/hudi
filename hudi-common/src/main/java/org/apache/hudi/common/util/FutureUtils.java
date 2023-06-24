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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * A utility class for future operation.
 */
public class FutureUtils {

  /**
   * Similar to {@link CompletableFuture#allOf(CompletableFuture[])} with a few important
   * differences:
   *
   * <ol>
   *  <li>Completes successfully as soon as *all* of the futures complete successfully</li>
   *  <li>Completes exceptionally as soon as *any* of the futures complete exceptionally</li>
   *  <li>In case it's completed exceptionally all the other futures not completed yet, will be
   *  cancelled</li>
   * </ol>
   *
   * @param futures list of {@link CompletableFuture}s
   */
  public static <T> CompletableFuture<List<T>> allOf(List<CompletableFuture<T>> futures) {
    CompletableFuture<Void> union = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

    futures.forEach(future -> {
      // NOTE: We add a callback to every future, to cancel all the other not yet completed futures,
      //       which will be providing for an early termination semantic: whenever any of the futures
      //       fail other futures will be cancelled and the exception will be returned as a result
      future.whenComplete((ignored, throwable) -> {
        if (throwable != null) {
          futures.forEach(f -> f.cancel(true));
          union.completeExceptionally(throwable);
        }
      });
    });

    return union.thenApply(aVoid ->
        futures.stream()
            // NOTE: This join wouldn't block, since all the
            //       futures are completed at this point.
            .map(CompletableFuture::join)
            .collect(Collectors.toList()));
  }
}
