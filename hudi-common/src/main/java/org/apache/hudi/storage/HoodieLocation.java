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

package org.apache.hudi.storage;

import java.io.Serializable;

public class HoodieLocation implements Serializable {
  String path;

  public HoodieLocation(String path) {
    this.path = path;
  }

  public HoodieLocation(String parent, String fileName) {
    this.path = parent + "/" + fileName;
  }

  public HoodieLocation(HoodieLocation parent, String fileName) {
    this.path = parent + "/" + fileName;
  }

  public HoodieLocation(HoodieLocation parent, HoodieLocation fileName) {
    this.path = parent + "/" + fileName;
  }

  public HoodieLocation getParent() {
    // return the parent
    return null;
  }

  public String getName() {
    // return the name after last slash
    return "";
  }

  public int depth() {
    return 0;
  }

  @Override
  public String toString() {
    return path;
  }
}
