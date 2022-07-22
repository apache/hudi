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

import static org.apache.hudi.TypeUtils.unsafeCast;

/**
 * Utility that could hold exclusively only either of (hence the name):
 * <ul>
 *     <li>Non-null value of type {@link L}</li>
 *     <li>Non-null value of type {@link R}</li>
 * </ul>
 *
 * @param <L> type of the "left" potential element
 * @param <R> type of the "right" potential element
 */
public abstract class Either<L, R> {

  @Nonnull
  protected abstract Object getValue();

  public final boolean isLeft() {
    return this instanceof EitherLeft;
  }

  public final boolean isRight() {
    return this instanceof EitherRight;
  }

  public R asRight() {
    ValidationUtils.checkArgument(isRight(), "Trying to access non-existent value of Either");
    EitherRight<L, R> right = unsafeCast(this);
    return right.getValue();
  }

  public L asLeft() {
    ValidationUtils.checkArgument(isLeft(), "Trying to access non-existent value of Either");
    EitherLeft<L, R> left = unsafeCast(this);
    return left.getValue();
  }

  public static <L, R> Either<L, R> right(R right) {
    return new EitherRight<>(right);
  }

  public static <L, R> Either<L, R> left(L left) {
    return new EitherLeft<>(left);
  }

  public static class EitherRight<L, R> extends Either<L, R> {
    private final R value;
    private EitherRight(@Nonnull R right) {
      this.value = right;
    }

    @Nonnull
    @Override
    protected R getValue() {
      return value;
    }
  }

  public static class EitherLeft<L, R> extends Either<L, R> {
    private final L value;
    private EitherLeft(@Nonnull L value) {
      this.value = value;
    }

    @Nonnull
    @Override
    protected L getValue() {
      return value;
    }
  }
}