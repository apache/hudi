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

package org.apache.hudi.common.table.timeline;

import java.io.Serializable;
import java.util.Comparator;

public interface InstantComparator extends Serializable {

  /**
   * Returns Comparator that only uses action for ordering taking into account equivalent actions.
   * @return {@link Comparator<HoodieInstant>}
   */
  Comparator<HoodieInstant> actionOnlyComparator();

  /**
   * Returns comparator that orders primarily based on timestamp and secondary ordering based on action and state.
   * @return {@link Comparator<HoodieInstant>}
   */
  Comparator<HoodieInstant> requestedTimeOrderedComparator();

  /**
   * Returns comparator that orders primarily based on completion time and secondary ordering based on {@link #requestedTimeOrderedComparator()}.
   * @return {@link Comparator<HoodieInstant>}
   */
  Comparator<HoodieInstant> completionTimeOrderedComparator();
}
