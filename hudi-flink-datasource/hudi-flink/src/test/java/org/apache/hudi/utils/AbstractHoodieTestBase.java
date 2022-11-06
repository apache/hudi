/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utils;

import org.apache.hudi.exception.HoodieException;

import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.lang.reflect.Field;
import java.util.Arrays;

import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;

/**
 * Hoodie base class for tests that run multiple tests and want to reuse the same Flink cluster.
 * Unlike {@link AbstractTestBase}, this class is designed to run with JUnit 5.
 */
public abstract class AbstractHoodieTestBase extends AbstractTestBase {

  private static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE = getMiniClusterFromParentClass();

  @BeforeAll
  public static void beforeAll() throws Exception {
    MINI_CLUSTER_RESOURCE.before();
  }

  @AfterAll
  public static void afterAll() {
    MINI_CLUSTER_RESOURCE.after();
  }

  @AfterEach
  public void afterEach() throws Exception {
    cleanupRunningJobs();
  }

  private static MiniClusterWithClientResource getMiniClusterFromParentClass() {
    String fieldFlink114 = "miniClusterResource";
    String fieldFlink115 = "MINI_CLUSTER_RESOURCE";
    Field miniClusterField = Arrays.stream(AbstractTestBase.class.getDeclaredFields())
        .filter(f -> isPublic(f.getModifiers()) && isStatic(f.getModifiers()))
        .filter(f -> fieldFlink114.equals(f.getName()) || fieldFlink115.equals(f.getName()))
        .findAny()
        .orElseThrow(() -> new NoSuchFieldError(String.format(
            "%s not found in %s",
            fieldFlink115,
            AbstractTestBase.class.getName())));
    try {
      return (MiniClusterWithClientResource) miniClusterField.get(null);
    } catch (IllegalAccessException e) {
      throw new HoodieException("Cannot access miniCluster field", e);
    }
  }
}
