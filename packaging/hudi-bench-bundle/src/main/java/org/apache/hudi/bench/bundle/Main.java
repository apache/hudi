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

package org.apache.hudi.bench.bundle;

import org.apache.hudi.common.util.ReflectionUtils;

/**
 * A simple main class to dump all classes loaded in current classpath
 *
 * This is a workaround for generating sources and javadoc jars for packaging modules. The maven plugins for generating
 * javadoc and sources plugins do not generate corresponding jars if there are no source files.
 *
 * This class does not have anything to do with Hudi but is there to keep mvn javadocs/source plugin happy.
 */
public class Main {

  public static void main(String[] args) {
    ReflectionUtils.getTopLevelClassesInClasspath(Main.class).forEach(System.out::println);
  }
}
