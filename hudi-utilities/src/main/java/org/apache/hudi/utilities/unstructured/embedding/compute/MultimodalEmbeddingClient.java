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

package org.apache.hudi.utilities.unstructured.embedding.compute;

/**
 * An {@link EmbeddingClient} that embeds images as well as text. Compute hands it image rows (modality {@code image}): each such
 * {@link EmbeddingInput} carries its stored input ({@link EmbeddingInput#storedImage()}) and, once image compute prepares it, the
 * resized model input ({@link EmbeddingInput#image()}), made lazily when the client asks for it, so at most one prepared image per
 * call in flight is held, never a batch of them.
 *
 * <p>Outcome contract for image inputs, on top of {@link EmbeddingClient}'s: when {@link PreparedImageSupplier#get()} throws, the
 * client answers that input with {@link ImageFailures#result} and goes on with the others. A refusal before decode
 * ({@link org.apache.hudi.utilities.unstructured.embedding.image.ImageRefusedException}), an unreadable header
 * ({@link org.apache.hudi.utilities.unstructured.embedding.image.ImageHeaderException}), corrupt pixel data
 * ({@link javax.imageio.IIOException}) or a body cut short is PERMANENT; a storage failure under the read
 * ({@link org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException} in the cause chain) is RETRYABLE; a malformed
 * stored-input reference fails the run ({@link EmbeddingComputePlatformException}).
 *
 * <p>A client that does not implement this interface embeds text only: compute never hands it an image row, and gives every such
 * row a permanent failure naming the provider instead.
 */
public interface MultimodalEmbeddingClient extends EmbeddingClient {
}
