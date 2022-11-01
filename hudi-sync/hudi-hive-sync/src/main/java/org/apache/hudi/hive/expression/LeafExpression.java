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

package org.apache.hudi.hive.expression;

/**
 * Expression that without any child expressions.
 */
public abstract class LeafExpression extends Expression {

  public LeafExpression() {
    super(null);
  }

  public static class Literal extends LeafExpression {

    private final String value;
    private final String type;

    public Literal(String value, String type) {
      this.value = value;
      this.type = type;
    }

    public String getValue() {
      return value;
    }

    public String getType() {
      return type;
    }
  }

  public static class NameExpression extends LeafExpression {

    private final String name;

    public NameExpression(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
