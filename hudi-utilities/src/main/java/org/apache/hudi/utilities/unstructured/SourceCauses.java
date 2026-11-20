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

package org.apache.hudi.utilities.unstructured;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/** Internal cause-chain traversal for source failures, stopping before an identity repeats. */
public final class SourceCauses {
  private SourceCauses() {
  }

  public static List<Throwable> getCauses(Throwable failure) {
    List<Throwable> causes = new ArrayList<>();
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable cause = failure; cause != null && visited.add(cause); cause = cause.getCause()) {
      causes.add(cause);
    }
    return causes;
  }

  public static boolean isCausedBy(Throwable failure, Class<? extends Throwable> type) {
    for (Throwable cause : getCauses(failure)) {
      if (type.isInstance(cause)) {
        return true;
      }
    }
    return false;
  }

  public static Throwable getRootCause(Throwable failure) {
    List<Throwable> causes = getCauses(failure);
    return causes.isEmpty() ? null : causes.get(causes.size() - 1);
  }
}
