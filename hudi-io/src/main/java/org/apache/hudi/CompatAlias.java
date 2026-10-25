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

package org.apache.hudi;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class or method that is retained at its old location/signature purely as a
 * compatibility alias for its relocated replacement, so that existing user configs and
 * code referencing the old fully-qualified name keep working across an upgrade.
 *
 * <p>Elements carrying this annotation hold no logic of their own, must not be used in
 * new code, and will be removed in a future major release once the deprecation window
 * has passed.
 */
@Documented
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.CLASS)
public @interface CompatAlias {
  /**
   * The relocated replacement to use instead of the annotated element.
   */
  Class<?> of();

  /**
   * The release in which the replacement was introduced and this alias was deprecated.
   */
  String since();
}
