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

package org.apache.hudi.common.index.vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Allocation-light bounded heap for vector candidate ordinals and approximate scores. */
final class VectorCandidateHeap {

  private long[] keys;
  private float[] scores;
  private int size;

  VectorCandidateHeap(int capacity) {
    this.keys = new long[capacity];
    this.scores = new float[capacity];
  }

  int size() {
    return size;
  }

  void growTo(int capacity) {
    if (capacity > keys.length) {
      keys = Arrays.copyOf(keys, capacity);
      scores = Arrays.copyOf(scores, capacity);
    }
  }

  boolean wouldAdmit(float score) {
    return size < keys.length || compare(score, Long.MIN_VALUE, scores[0], keys[0]) <= 0;
  }

  void offer(long key, float score) {
    if (keys.length == 0) {
      return;
    }
    if (size < keys.length) {
      keys[size] = key;
      scores[size] = score;
      siftUp(size++);
    } else if (compare(score, key, scores[0], keys[0]) < 0) {
      keys[0] = key;
      scores[0] = score;
      siftDown(0);
    }
  }

  List<Entry> entriesBestFirst() {
    List<Entry> entries = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      entries.add(new Entry(keys[i], scores[i]));
    }
    entries.sort((left, right) -> compare(left.score, left.key, right.score, right.key));
    return entries;
  }

  private void siftUp(int child) {
    while (child > 0) {
      int parent = (child - 1) >>> 1;
      if (!worse(child, parent)) {
        return;
      }
      swap(child, parent);
      child = parent;
    }
  }

  private void siftDown(int parent) {
    while (true) {
      int left = (parent << 1) + 1;
      if (left >= size) {
        return;
      }
      int right = left + 1;
      int worseChild = right < size && worse(right, left) ? right : left;
      if (!worse(worseChild, parent)) {
        return;
      }
      swap(parent, worseChild);
      parent = worseChild;
    }
  }

  private boolean worse(int leftIndex, int rightIndex) {
    return compare(scores[leftIndex], keys[leftIndex], scores[rightIndex], keys[rightIndex]) > 0;
  }

  private void swap(int left, int right) {
    long key = keys[left];
    float score = scores[left];
    keys[left] = keys[right];
    scores[left] = scores[right];
    keys[right] = key;
    scores[right] = score;
  }

  private static int compare(float leftScore, long leftKey, float rightScore, long rightKey) {
    int scoreComparison = Float.compare(leftScore, rightScore);
    return scoreComparison != 0 ? scoreComparison : Long.compare(leftKey, rightKey);
  }

  static final class Entry {
    final long key;
    final float score;

    private Entry(long key, float score) {
      this.key = key;
      this.score = score;
    }
  }
}
