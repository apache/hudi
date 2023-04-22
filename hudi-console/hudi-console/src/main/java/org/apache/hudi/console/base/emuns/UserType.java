/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.console.base.emuns;

import com.baomidou.mybatisplus.annotation.EnumValue;

import java.util.Arrays;

/** The user type. */
public enum UserType {

  /** The admin of HUDI-Console. */
  ADMIN(1),

  /** The user of HUDI-Console. */
  USER(2);

  @EnumValue private final int code;

  UserType(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static UserType of(Integer code) {
    return Arrays.stream(values()).filter((x) -> x.code == code).findFirst().orElse(null);
  }
}
