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

package org.apache.hudi.common.function;

import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;

/**
 * A serializable functional interface that represents a function taking an input value
 * and returning a key-value pair result. This interface is designed to be used in distributed computing
 * scenarios where functions need to be serialized and transmitted across network boundaries.
 *
 * <p>This interface extends {@link Serializable} and is marked as a functional interface,
 * making it suitable for use with lambda expressions and method references in distributed
 * processing frameworks.</p>
 *
 * <p>The function signature follows the pattern: I → (K, V), where I is the input type,
 * K is the output key type, and V is the output value type.</p>
 *
 * <p><strong>Example usage:</strong></p>
 * <pre>{@code
 * // Create a function that splits a string into key-value pair
 * SerializableFunctionPairOut<String, String, Integer> splitFunction =
 *     str -> Pair.of(str.split(":")[0], Integer.parseInt(str.split(":")[1]));
 *
 * // Use in a stream or processing pipeline
 * Pair<String, Integer> result = splitFunction.call("user:123"); // Returns Pair("user", 123)
 * }</pre>
 *
 * <p><strong>Note:</strong> The {@code call} method is declared to throw {@link Exception},
 * allowing implementations to handle various error conditions that may occur during
 * processing.</p>
 *
 * @param <I> the type of the input parameter
 * @param <K> the type of the output key
 * @param <V> the type of the output value
 */
@FunctionalInterface
public interface SerializableFunctionPairOut<I, K, V> extends Serializable {
  Pair<K, V> call(I t) throws Exception;
}
