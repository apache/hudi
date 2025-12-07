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

package org.apache.hudi.common.util.collection;

import lombok.Getter;

/**
 * (NOTE: Adapted from Apache commons-lang3)
 * <p>
 * An immutable pair consisting of two {@code Object} elements.
 * </p>
 *
 * <p>
 * Although the implementation is immutable, there is no restriction on the objects that may be stored. If mutable
 * objects are stored in the pair, then the pair itself effectively becomes mutable. The class is also {@code final}, so
 * a subclass can not add undesirable behaviour.
 * </p>
 *
 * <p>
 * #ThreadSafe# if both paired objects are thread-safe
 * </p>
 *
 * @param <L> the left element type
 * @param <R> the right element type
 */
public final class ImmutablePair<L, R> extends Pair<L, R> {

  /**
   * Serialization version.
   */
  private static final long serialVersionUID = 4954918890077093841L;

  /**
   * Left object.
   */
  @Getter
  public final L left;

  /**
   * Right object.
   */
  @Getter
  public final R right;

  /**
   * <p>
   * Obtains an immutable pair of from two objects inferring the generic types.
   * </p>
   *
   * <p>
   * This factory allows the pair to be created using inference to obtain the generic types.
   * </p>
   *
   * @param <L> the left element type
   * @param <R> the right element type
   * @param left the left element, may be null
   * @param right the right element, may be null
   * @return a pair formed from the two parameters, not null
   */
  public static <L, R> ImmutablePair<L, R> of(final L left, final R right) {
    return new ImmutablePair<L, R>(left, right);
  }

  /**
   * Create a new pair instance.
   *
   * @param left the left value, may be null
   * @param right the right value, may be null
   */
  public ImmutablePair(final L left, final R right) {
    super();
    this.left = left;
    this.right = right;
  }

  /**
   * <p>
   * Throws {@code UnsupportedOperationException}.
   * </p>
   *
   * <p>
   * This pair is immutable, so this operation is not supported.
   * </p>
   *
   * @param value the value to set
   * @return never
   * @throws UnsupportedOperationException as this operation is not supported
   */
  @Override
  public R setValue(final R value) {
    throw new UnsupportedOperationException();
  }

}
