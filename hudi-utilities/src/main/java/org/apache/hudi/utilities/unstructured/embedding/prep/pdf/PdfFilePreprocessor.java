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

package org.apache.hudi.utilities.unstructured.embedding.prep.pdf;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.prep.Chunk;
import org.apache.hudi.utilities.unstructured.embedding.prep.FilePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessingException;
import org.apache.hudi.utilities.unstructured.embedding.prep.SourceKind;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * The production pdf {@link FilePreprocessor}: adapts the {@link PdfChunker} contract to the embedding framework.
 * <ul>
 *   <li>an OK {@link PdfChunk} becomes a text {@link Chunk} with its token count and the aux {@code page_start},
 *       {@code page_end}, {@code overlap_chars}; the LAST chunk of a file that lost content also carries
 *       {@code prep_loss} = the {@code PAGES_LOST …} summary;</li>
 *   <li>a FAILED chunker marker becomes a {@link PreprocessingException} carrying its status detail (the framework
 *       quarantines the file, stage prep); a NO_TEXT marker ends the iterator without a chunk (the framework quarantines the
 *       file as empty);</li>
 *   <li>the framework numbers the chunks in emission order, which is the chunker's {@code chunk_id} order.</li>
 * </ul>
 * {@link #name()} is the versioned implementation identity of the preprocessing, which identifies it in logs and error
 * text: it changes whenever {@link #IMPLEMENTATION_VERSION} is bumped, which every change to emitted records must do. The
 * name also records the PDF text extraction logic ({@link PdfBoxPageSource#LOGIC_VERSION}), never the PDFBox jar version: a
 * PDFBox upgrade that changes the emitted records must bump that constant; the automatic checks are the pinned-records tests
 * over synthetic PDFs.
 */
public class PdfFilePreprocessor implements FilePreprocessor {

  /** Bump on ANY change to the records this preprocessor emits for the same bytes and config. */
  public static final int IMPLEMENTATION_VERSION = 5;  // 5: bounded PDFBox tokens; 4: durable decode loss
  public static final String NAME_PREFIX = "pdf-chunker/";
  public static final String AUX_PAGE_START = "page_start";
  public static final String AUX_PAGE_END = "page_end";
  public static final String AUX_OVERLAP_CHARS = "overlap_chars";
  /**
   * Aux field of the last chunk of a file that lost content: a diagnostic summary ({@code PAGES_LOST …}) for operators. Its text
   * is opaque and unversioned, and may change between builds; nothing may parse it or depend on its format.
   */
  public static final String AUX_PREP_LOSS = "prep_loss";
  private final PdfChunker chunker;
  private final String name;
  private final PdfChunkerConfig.Cache config = new PdfChunkerConfig.Cache();

  public PdfFilePreprocessor() {
    this(new PdfBoxChunker());
  }

  PdfFilePreprocessor(PdfChunker chunker) {
    this.chunker = chunker;
    this.name = NAME_PREFIX + IMPLEMENTATION_VERSION + ";" + PdfBoxPageSource.LOGIC_VERSION;
  }

  /** {@code pdf-chunker/<IMPLEMENTATION_VERSION>;<PdfBoxPageSource.LOGIC_VERSION>}. */
  @Override
  public String name() {
    return name;
  }

  @Override
  public SourceKind sourceKind() {
    return SourceKind.DOCUMENT;
  }

  @Override
  public void validateConfig(TypedProperties props, Tokenizer tokenizer) {
    PdfChunkerConfig.from(props, tokenizer);
  }

  @Override
  public ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props) {
    String logPath = file.getSourcePathSha() + "." + file.getFileExtension();
    PdfChunkerConfig cfg;
    try {
      cfg = config.get(props, tokenizer);
    } catch (IllegalArgumentException e) {
      // invalid props are the job's configuration error: the round fails instead of every file being quarantined
      throw new PreprocessorConfigurationException(e.getMessage(), e);
    }
    ClosableIterator<PdfChunk> chunks = chunker.open(bytes::open, new PdfFileMeta(file.getSourcePathSha(), logPath,
        file.getFileSize()), cfg);
    return new ChunkAdapter(chunks);
  }

  /** Maps contract records onto framework chunks. */
  private static final class ChunkAdapter implements ClosableIterator<Chunk> {
    private final ClosableIterator<PdfChunk> chunks;
    private Chunk next;
    private boolean ended;

    ChunkAdapter(ClosableIterator<PdfChunk> chunks) {
      this.chunks = chunks;
    }

    @Override
    public boolean hasNext() {
      if (next == null && !ended) {
        if (!chunks.hasNext()) {
          ended = true;
        } else {
          PdfChunk chunk = chunks.next();
          switch (chunk.status()) {
            case OK:
              next = toChunk(chunk);
              break;
            case FAILED:
              ended = true;
              throw new PreprocessingException(chunk.statusDetail());
            case NO_TEXT:
              ended = true;                              // the framework quarantines the file as empty
              break;
            default:
              throw new UnsupportedOperationException("Unknown PdfChunk status " + chunk.status());
          }
        }
      }
      return next != null;
    }

    @Override
    public Chunk next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Chunk chunk = next;
      next = null;
      return chunk;
    }

    @Override
    public void close() {
      chunks.close();
    }

    private static Chunk toChunk(PdfChunk chunk) {
      Map<String, String> aux = new HashMap<>();
      aux.put(AUX_PAGE_START, String.valueOf(chunk.pageStart()));
      aux.put(AUX_PAGE_END, String.valueOf(chunk.pageEnd()));
      aux.put(AUX_OVERLAP_CHARS, String.valueOf(chunk.overlapChars()));
      if (chunk.statusDetail() != null) {
        aux.put(AUX_PREP_LOSS, chunk.statusDetail());
      }
      return new Chunk(EmbeddingTableSchema.MODALITY_TEXT, chunk.text(), chunk.tokenCount(), aux);
    }
  }
}
