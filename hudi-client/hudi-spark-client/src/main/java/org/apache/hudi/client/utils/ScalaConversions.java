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

package org.apache.hudi.client.utils;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ScalaConversions {

  public static <T> List<T> toList(scala.collection.Seq<T> seq) {
    return scala.collection.JavaConverters.seqAsJavaListConverter(seq).asJava();
  }

  @SafeVarargs
  public static <T> scala.collection.Seq<T> toSeq(T... javaItems) {
    return toSeq(Stream.of(javaItems));
  }

  public static <T> scala.collection.Seq<T> toSeq(Stream<T> javaStream) {
    return scala.collection.JavaConverters.asScalaIteratorConverter(javaStream.iterator()).asScala().toSeq();
  }

  public static scala.collection.Map<String, String> toScalaMap(Properties props) {
    Map<String, String> map = props.entrySet().stream()
        .collect(Collectors.toMap(e -> String.valueOf(e.getKey()), e -> String.valueOf(e.getValue())));
    return toScalaMap(map);
  }

  public static scala.collection.Map<String, String> toScalaMap(Map<String, String> map) {
    return scala.collection.JavaConverters.mapAsScalaMapConverter(map).asScala();
  }

}
