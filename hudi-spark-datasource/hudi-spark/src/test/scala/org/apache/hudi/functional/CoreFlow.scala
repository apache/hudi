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

package org.apache.hudi.functional

import org.scalatest.Tag

/**
 * ScalaTest tag for the curated "core flow" subset that runs on every Spark
 * version in CI (via the {@code core-tests} Maven profile), as opposed to the
 * full suite that runs only on the latest Spark major versions.
 *
 * The tag name matches the {@link SparkSQLCoreFlow} Java {@code @TagAnnotation}
 * so that class-level {@code @SparkSQLCoreFlow} and per-test
 * {@code taggedAs(CoreFlow)} are selected by the same scalatest
 * {@code tagsToInclude}/{@code tagsToExclude} value.
 */
object CoreFlow extends Tag("org.apache.hudi.functional.SparkSQLCoreFlow")
