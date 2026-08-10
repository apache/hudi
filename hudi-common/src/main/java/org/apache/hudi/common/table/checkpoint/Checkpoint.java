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

package org.apache.hudi.common.table.checkpoint;

import lombok.AccessLevel;
import lombok.Getter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Class for representing checkpoint
 */
@Getter
public abstract class Checkpoint implements Serializable {

  public static final String CHECKPOINT_IGNORE_KEY = "deltastreamer.checkpoint.ignore_key";

  protected String checkpointKey;
  protected String checkpointResetKey;
  protected String checkpointIgnoreKey;
  // These are extra props to be written to the commit metadata
  @Getter(AccessLevel.NONE)
  protected Map<String, String> extraProps = new HashMap<>();

  public Checkpoint setCheckpointKey(String newKey) {
    checkpointKey = newKey;
    return this;
  }

  public abstract Map<String, String> getCheckpointCommitMetadata(String overrideResetKey,
                                                                  String overrideIgnoreKey);

  // Not using Lombok @EqualsAndHashCode/@ToString here: this class is subclassed, and we rely on
  // the runtime subtype - exact-class matching in equals() and getClass().getSimpleName() in toString().
  // Lombok would bake in the declaring class (Checkpoint) and switch equals() to instanceof.
  @Override
  public int hashCode() {
    return Objects.hashCode(checkpointKey);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Checkpoint that = (Checkpoint) o;
    return Objects.equals(checkpointKey, that.checkpointKey);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{checkpointKey='" + checkpointKey + "'}";
  }
}
