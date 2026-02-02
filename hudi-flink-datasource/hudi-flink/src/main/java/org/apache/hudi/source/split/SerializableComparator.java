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

package org.apache.hudi.source.split;

import java.io.Serializable;
import java.util.Comparator;

/**
 * A serializable comparator interface for objects that need to be compared
 * in a distributed environment.
 *
 * <p>This interface extends both {@link Comparator} and {@link Serializable},
 * making it suitable for use in distributed systems where comparators need to be
 * serialized and sent across network boundaries.
 *
 * @param <T> the type of objects that may be compared by this comparator
 */
public interface SerializableComparator<T> extends Comparator<T>, Serializable {}
