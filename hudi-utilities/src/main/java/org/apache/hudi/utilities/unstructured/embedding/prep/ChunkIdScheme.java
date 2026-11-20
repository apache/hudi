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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import java.io.Serializable;

/**
 * Turns the dense ordinal of a chunk among its file's chunks of the SAME modality (0, 1, 2, … in extraction order) into its
 * {@code chunk_id}; each modality is numbered on its own. An implementation must be STRICTLY increasing in the
 * ordinal, so for two chunks of one file and one modality {@code a.chunk_id < b.chunk_id} holds exactly when {@code a} was
 * extracted before {@code b}; ids of different modalities are not ordered against each other. Marker rows keep their
 * fixed id. Data prep numbers chunks with {@link SequentialChunkIdScheme}.
 */
public interface ChunkIdScheme extends Serializable {

  long chunkId(long ordinal);
}
