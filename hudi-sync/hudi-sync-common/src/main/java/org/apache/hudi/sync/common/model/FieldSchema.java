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

package org.apache.hudi.sync.common.model;

import org.apache.hudi.common.util.Option;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@AllArgsConstructor
@Getter
@Setter
public class FieldSchema {

  private final String name;
  private String type;
  private Option<String> comment;

  public FieldSchema(String name, String type) {
    this(name, type, Option.empty());
  }

  public FieldSchema(String name, String type, String comment) {
    this(name, type, Option.ofNullable(comment));
  }

  public String getCommentOrEmpty() {
    return comment.orElse("");
  }

  public boolean updateComment(FieldSchema another) {
    if (Objects.equals(name, another.getName())
        && !Objects.equals(getCommentOrEmpty(), another.getCommentOrEmpty())) {
      setComment(another.getComment());
      return true;
    } else {
      return false;
    }
  }
}
