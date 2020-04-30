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

package org.apache.hudi.utilities.mongo;

/**
 * The constants from the debezium operation values.
 */
public enum Operation {
  /**
   * The operation that read the current state of a record, most typically during snapshots.
   */
  READ("r"),

  /**
   * An operation that resulted in a new record being created in the source.
   */
  CREATE("c"),

  /**
   * An operation that resulted in an existing record being updated in the source.
   */
  UPDATE("u"),

  /**
   * An operation that resulted in an existing record being removed from or deleted in the source.
   */
  DELETE("d");

  private final String code;

  private Operation(String code) {
    this.code = code;
  }

  public static Operation forCode(String code) {
    for (Operation op : Operation.values()) {
      if (op.code().equals(code)) {
        return op;
      }
    }
    return null;
  }

  public String code() {
    return code;
  }
}
