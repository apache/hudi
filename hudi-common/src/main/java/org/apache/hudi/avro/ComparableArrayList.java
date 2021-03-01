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

package org.apache.hudi.avro;

import java.util.ArrayList;

public class ComparableArrayList<E> extends ArrayList<E> implements Comparable<E> {
  @Override
  public int compareTo(Object o) {
    ArrayList to = ((ArrayList)o);
    for (int i = 0; i < to.size(); i++) {
      if (this.size() <= i) {
        return -1;
      }
      Comparable thisData = (Comparable)this.get(i);
      if (thisData == null) {
        return -1;
      } else {
        int cmpVal = thisData.compareTo(to.get(i));
        if (cmpVal == 0) {
          continue;
        } else {
          return cmpVal;
        }
      }
    }
    if (this.size() > to.size()) {
      return 1;
    }
    return 0;
  }
}
