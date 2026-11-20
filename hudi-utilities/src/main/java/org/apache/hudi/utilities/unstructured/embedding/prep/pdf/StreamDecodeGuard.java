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

import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.prep.PrepFailures;
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;

import org.apache.pdfbox.contentstream.PDContentStream;
import org.apache.pdfbox.cos.COSArray;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSObject;
import org.apache.pdfbox.cos.COSObjectKey;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.filter.Filter;
import org.apache.pdfbox.filter.FilterFactory;
import org.apache.pdfbox.filter.FlateFilterDecoderStream;
import org.apache.pdfbox.io.RandomAccessInputStream;
import org.apache.pdfbox.io.RandomAccessRead;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.util.Matrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;

/**
 * The decoded-size pre-check of the in-JVM page source: before a page is extracted, decodes once, through a
 * counting sink, exactly the streams PDFBox 3.0.8 text extraction reads for that page — content streams, form XObjects
 * (recursively, levels 1..50), and the fonts named in {@code /Font} or an {@code /ExtGState}'s {@code /Font}: their font
 * files, {@code /ToUnicode}, a CMap {@code /Encoding}, Type0 descendants' font files and {@code /CIDToGIDMap}, and every
 * Type3 {@code /CharProcs} entry.
 *
 * <p>Only the resources the content draws are checked, as only those are read by PDFBox: the forms named by a
 * {@code Do}, the fonts named by a {@code Tf} and the ExtGStates named by a {@code gs} in the page's /Contents (resolved in
 * {@link PDPage#getResources()}, so inherited resources are covered when drawn) and, recursively, in each drawn form
 * (resolved in the form's own /Resources, or in its caller's when it has none, as PDFStreamEngine does). An entry a
 * page lists but never draws, such as an oversized form or font shared through inherited resources, does not fail the
 * page. The names come from PDFBox's own content parser ({@link DrawnResources#collect}), run over the page (the same
 * /Contents sequence extraction parses) and over each drawn form, after this guard has checked those streams: the parse
 * is the one extraction does next, so no name PDFBox resolves is missed.
 *
 * <p>That parse is a {@link BoundedContentParser}, which holds the structure budgets (operands pending at an operator,
 * objects inside one array or dictionary operand, bytes of one token) on the tokens PDFBox itself reads. The pre-scan
 * evaluates the same budgets on the guard's own decoding and tokenization, which can differ from PDFBox's: on unusual
 * token sequences, and for a page whose /Contents is a single /FlateDecode stream, which PDPage parses without its
 * /DecodeParms predictor (that parse also gets a {@code maxDecodedBytes} budget for the whole content). A budget passed in
 * either fails the page with {@code DECODE_LIMIT}. Drawn forms and the CharProcs of checked Type3 fonts (which extraction
 * parses for glyph widths and boxes) get the same budgeted parse, once per document; one it refuses is cached as failed.
 * A parse that fails for another reason, or a form whose names were not kept, checks every entry of the dictionary.
 *
 * <ul>
 *   <li>a stream decoding to more than {@code maxDecodedBytes} fails the page with {@code DECODE_LIMIT}, and so do, without
 *       decoding, an image codec ({@code DCT}, {@code JPX}, {@code JBIG2}, {@code CCITTFax}) in a checked stream and a
 *       Flate/LZW predictor whose {@code /DecodeParms} row exceeds {@value #MAX_PREDICTOR_ROW_BYTES} bytes (PDFBox allocates
 *       two such rows before the first byte is decoded);</li>
 *   <li>a stream that fails to decode for any other reason is skipped, as PDFBox skips it: PDFBox marks a font whose
 *       font file, ToUnicode, CMap encoding, CIDToGIDMap or CharProc fails to decode damaged and never materialises
 *       that stream, leaves out a /Contents stream that fails to decode ("malformed substream of content stream
 *       skipped"), and only warns when a form drawn by {@code Do} fails; the page scanner forgets the bytes such a
 *       /Contents stream fed it. A page that loses a /Contents stream or a drawn form this way is logged once at WARN
 *       with the page number and the skipped streams, since its extracted text may be incomplete;</li>
 *   <li>more than {@value #MAX_STREAMS_PER_PAGE} distinct unchecked non-CharProc streams on one page, or more than
 *       {@value #MAX_CHAR_PROCS_PER_FONT} CharProcs in one Type3 font, fail the page with {@code PAGE_ERROR} — a cap never
 *       silently skips;</li>
 *   <li>indirect streams are cached per document by {@link COSObjectKey} in a PASSED, a FAILED and an UNDECODABLE set
 *       bounded by the streams visited; a form is re-expanded only when reached at a shallower level than
 *       before.</li>
 * </ul>
 * Every checked stream PDFBox parses is also pre-scanned, while it is decoded (and read raw when unfiltered), by a
 * {@link StreamStructureScanner} chosen by its {@link Kind}: a content stream, form or CharProc for the operands PDFBox
 * keeps between operators, the elements of one array operand and the bytes of one string or inline-image body (held to
 * {@code maxDecodedBytes} even in an unfiltered stream, as PDFBox copies such a body onto the heap), a ToUnicode or CMap
 * encoding for its bfrange span, an embedded Type1 font file for its declared /Subrs count (only /Subrs: fontbox's
 * readCharStrings does not preallocate by the declared /CharStrings count). A scanner past its cap fails the page with
 * {@code DECODE_LIMIT} (structures that expand while PARSED pass the decoded-size cap). The streams of a
 * page's /Contents array are scanned as the one sequence PDFBox parses: one operand scanner, a newline fed after each stream
 * (PDPage's DELIMITER), every stream scanned on every page that lists it, and nothing cached from such a scan, because
 * its outcome depends on its neighbours. PDFBox decodes every filtered stream of a /Contents array onto the heap and
 * holds them together while the page is parsed, so their decoded lengths, one per listing, share {@code maxDecodedBytes}
 * too.
 *
 * Heap-copied font streams also share a page-local decoded-byte budget. Each indirect font dictionary charges once
 * per page, while direct dictionaries charge per occurrence: each time the walk of a drawn resource dictionary reaches
 * one (two forms selecting it through one shared dictionary charge twice, as PDFBox builds a copy for each). Distinct
 * dictionaries sharing a stream charge separately;
 * cached checks charge again for each dictionary and page. Aggregate refusals are never cached as stream failures.
 * Actual font materializations are also charged immediately before extraction applies Tf or gs. Tf follows PDFBox's
 * indirect-font and direct-resource-name caches; every gs application charges a new copy, including repeated form calls.
 * Type3 own Resources are outside this admission graph; CharProc checks remain unchanged.
 *
 * Multi-filter chains pass through temp files capped at {@code maxDecodedBytes} in the staging dir; heap use is one
 * buffer. A temp file that cannot be created, written or read back is the executor's local disk, not the page: it is a
 * {@link PrepPlatformException} ({@link ScratchFiles}), never a {@code PAGE_ERROR}, and fails the round (class P).
 * One instance per document, used by one thread.
 */
final class StreamDecodeGuard {

  private static final Logger LOG = LoggerFactory.getLogger(StreamDecodeGuard.class);
  private static final String STREAM = "stream ";

  static final int MAX_STREAMS_PER_PAGE = 4096;
  static final int MAX_CHAR_PROCS_PER_FONT = 1024;
  /** PDFBox stops expanding forms below this nesting level, so deeper forms are never decoded by it. */
  static final int MAX_FORM_LEVEL = 50;
  /**
   * Drawn-resource names kept for cached forms per document (each cached form counts at least 1); a form whose names
   * were not kept checks every resource when it is met again.
   */
  static final int MAX_CACHED_DRAWN_NAMES = 65_536;
  /** Skipped stream keys named in one page's WARN line. */
  static final int MAX_LOGGED_SKIPS = 8;
  /** Characters of one decode error kept in that line. */
  static final int MAX_LOGGED_ERROR_CHARS = 160;
  private static final int BUFFER = 64 * 1024;
  /** Filters that run PDFBox's Predictor, which allocates two rows sized from /DecodeParms before decoding. */
  private static final Set<COSName> PREDICTOR_FILTERS = new HashSet<>(Arrays.asList(COSName.FLATE_DECODE,
      COSName.FLATE_DECODE_ABBREVIATION, COSName.LZW_DECODE, COSName.LZW_DECODE_ABBREVIATION));
  /** Largest predictor row accepted; a legitimate content, font or CMap stream needs a few KiB at most. */
  static final int MAX_PREDICTOR_ROW_BYTES = 1 << 20;
  /** Image codecs allocate whole rasters before a byte reaches a sink; none is legitimate in a checked stream. */
  private static final Set<COSName> IMAGE_CODECS = new HashSet<>(Arrays.asList(COSName.DCT_DECODE,
      COSName.DCT_DECODE_ABBREVIATION, COSName.JPX_DECODE, COSName.JBIG2_DECODE, COSName.CCITTFAX_DECODE,
      COSName.CCITTFAX_DECODE_ABBREVIATION));

  private final long maxDecodedBytes;
  private final long maxPageFontBytes;
  private final File tempDir;
  /** What PDFBox parses a stream's decoded bytes as; picks the structure pre-scan. */
  enum Kind {
    CONTENT, CMAP, TYPE1,
    /**
     * Not pre-scanned, but copied onto the heap whole, filtered or not: CIDToGIDMap (PDCIDFont.readCIDToGIDMap)
     * and FontFile2/FontFile3, whose views fontbox's TTFParser and CFFParser copy into one byte[].
     */
    OPAQUE_COPIED
  }

  private final Map<COSObjectKey, EnumMap<Kind, Long>> passed = new HashMap<>();
  private final Set<COSObjectKey> failed = new HashSet<>();
  /** Streams that failed to decode for a reason other than a cap. */
  private final Set<COSObjectKey> undecodable = new HashSet<>();
  /** Single scalar-Flate page contents use a different decoder, so its refusals cannot poison ordinary stream views. */
  private final Set<COSObjectKey> streamingContentFailed = new HashSet<>();
  private final Set<COSObjectKey> streamingContentUndecodable = new HashSet<>();
  /** What each passed form draws, within {@link #MAX_CACHED_DRAWN_NAMES}. */
  private final Map<COSObjectKey, DrawnResources> drawnByStream = new HashMap<>();
  /** Forms and CharProcs whose budgeted PDFBox parse passed; never parsed again in this document. */
  private final Set<COSObjectKey> parsedWithinBudgets = new HashSet<>();
  private long cachedDrawnNames;
  /** Resource-dictionary lookups and fallback entries the walks have examined, for tests of the walk's cost. */
  private long resourceEntriesVisited;
  private final Consumer<String> warn;
  private long decodedStreams;

  // per page
  private int visited;
  private Set<COSObjectKey> chargedFontObjects;
  private Set<COSStream> chargedFontStreams;
  private int fontVisits;
  private FontBytes fontBytes;
  /** Direct Tf fonts are cached by resource instance and name; each form call may create a new resources instance. */
  private Map<PDResources, Set<COSName>> materializedDirectFonts;
  /** The page's single scalar-Flate stream, whose parser ignores DecodeParms; null for all other representations. */
  private COSStream streamingFlateContent;
  private boolean pageContentsArray;
  /** Per form, per resource dictionary its names resolve in, the shallowest level it was expanded at. */
  private Map<COSStream, Map<COSDictionary, Integer>> formLevels;
  /** Distinct content streams (/Contents or drawn forms) skipped on this page because they cannot be decoded. */
  private int skippedContent;
  private Set<COSObjectKey> skippedContentKeys;
  /** The first {@link #MAX_LOGGED_SKIPS} of them, for the log. */
  private List<String> skippedContentNames;
  private StreamStructureScanner.Operands pageOperands;
  /** Decoded bytes of the filtered /Contents streams of the page so far, each listing counted. */
  private long pageContentBytes;
  private int pageContentStreams;
  private int undecodableContentStreams;

  StreamDecodeGuard(long maxDecodedBytes, File tempDir) {
    this(maxDecodedBytes, EmbeddingTableConfig.DEFAULT_PDF_MAX_PAGE_FONT_BYTES, tempDir);
  }

  StreamDecodeGuard(long maxDecodedBytes, long maxPageFontBytes, File tempDir) {
    this(maxDecodedBytes, maxPageFontBytes, tempDir, LOG::warn);
  }

  /** @param warn where the once-per-page WARN about skipped undecodable content goes */
  StreamDecodeGuard(long maxDecodedBytes, long maxPageFontBytes, File tempDir, Consumer<String> warn) {
    this.maxDecodedBytes = maxDecodedBytes;
    this.maxPageFontBytes = maxPageFontBytes;
    this.tempDir = tempDir;
    this.warn = warn;
  }

  /** Streams decoded so far (each indirect stream at most once per document). */
  long decodedStreams() {
    return decodedStreams;
  }

  /** Resource-dictionary entries the resource walks have looked at in this document. */
  long resourceEntriesVisited() {
    return resourceEntriesVisited;
  }

  /** Content streams whose drawn names are cached for the rest of the document. */
  int cachedDrawnStreams() {
    return drawnByStream.size();
  }

  /** {@link #checkPage(PDPage, int)} for a page whose number is not known; its log names no page number. */
  int checkPage(PDPage page) {
    return checkPage(page, 0);
  }

  /**
   * @param pageNumber the page's 1-based number in the document, for the log; 0 when not known
   * @return content streams skipped on a partially decodable page
   * @throws PageFailure {@code DECODE_ERROR}, {@code DECODE_LIMIT} or {@code PAGE_ERROR} when the page must not be extracted
   * @throws PrepPlatformException when a temp file of the guard cannot be created, written or read back
   */
  int checkPage(PDPage page, int pageNumber) {
    visited = 0;
    pageContentBytes = 0;
    pageContentStreams = 0;
    undecodableContentStreams = 0;
    chargedFontObjects = new HashSet<>();
    fontVisits = 0;
    fontBytes = new FontBytes(maxPageFontBytes);
    formLevels = new IdentityHashMap<>();
    skippedContent = 0;
    skippedContentKeys = new HashSet<>();
    skippedContentNames = new ArrayList<>();
    // by design: the try holds the page's scanner; try-with-resources would run its close() on the failure path too, a behaviour
    // change; the finally only drops references
    try {
      COSDictionary pageDict = page.getCOSObject();
      COSBase contents = pageDict.getItem(COSName.CONTENTS);
      COSBase resolved = resolve(contents);
      pageContentsArray = resolved instanceof COSArray;
      streamingFlateContent = resolved instanceof COSStream && COSName.FLATE_DECODE.equals(((COSStream) resolved).getFilters())
          ? (COSStream) resolved : null;
      pageOperands = new StreamStructureScanner.Operands("the page content", false, maxDecodedBytes);
      if (resolved instanceof COSStream) {
        checkStream(contents, (COSStream) resolved, false, false, Kind.CONTENT);
      } else if (resolved instanceof COSArray) {
        COSArray array = (COSArray) resolved;
        for (int i = 0; i < array.size(); i++) {
          COSBase item = array.get(i);
          COSBase stream = resolve(item);
          if (stream instanceof COSStream) {
            checkStream(item, (COSStream) stream, false, false, Kind.CONTENT);
          }
        }
      }
      if (pageContentStreams > 0 && undecodableContentStreams == pageContentStreams) {
        warnSkippedContent(pageNumber);
        throw new PageFailure(PdfFailureCode.DECODE_ERROR, "all " + pageContentStreams + " content streams failed to decode");
      }
      pageOperands.finish();
      pageOperands = null;
      DrawnResources drawn = drawnBy(page, readsUnpredictedFlate(page) ? maxDecodedBytes : Long.MAX_VALUE);
      if (page.getResources() != null) {
        walkResources(page.getResources().getCOSObject(), 0, drawn);
      }
      warnSkippedContent(pageNumber);
      return undecodableContentStreams;
    } catch (PageFailure | PrepPlatformException e) {
      throw e;
    } catch (IOException | RuntimeException e) {
      throw new PageFailure(PdfFailureCode.PAGE_ERROR, "decode guard: " + e.getClass().getName() + ": " + e.getMessage(), e);
    } finally {
      formLevels = null;
      skippedContentKeys = null;
      skippedContentNames = null;
      chargedFontObjects = null;
      chargedFontStreams = null;
      fontBytes = null;
      pageOperands = null;
      streamingFlateContent = null;
    }
  }

  /** Starts a fresh budget for the font objects extraction actually creates, after static stream admission. */
  void beginFontMaterializations() {
    visited = 0;
    chargedFontObjects = new HashSet<>();
    materializedDirectFonts = new IdentityHashMap<>();
    fontVisits = 0;
    fontBytes = new FontBytes(maxPageFontBytes);
  }

  /** Releases resource identities after a successful or failed page extraction. */
  void endFontMaterializations() {
    chargedFontObjects = null;
    materializedDirectFonts = null;
    chargedFontStreams = null;
    fontBytes = null;
  }

  /** Called before PDFBox materializes a font; gs does not use PDResources' font cache. */
  void checkFontOperation(PDResources resources, COSName name, boolean graphicsState) throws IOException {
    if (resources == null) {
      return;
    }
    COSDictionary entries = dictionary(resources.getCOSObject().getItem(graphicsState ? COSName.EXT_G_STATE : COSName.FONT));
    if (entries == null) {
      return;
    }
    if (graphicsState) {
      COSDictionary state = dictionary(entries.getItem(name));
      COSBase setting = state == null ? null : resolve(state.getItem(COSName.FONT));
      if (setting instanceof COSArray && ((COSArray) setting).size() > 0) {
        checkFont(((COSArray) setting).get(0), false);
      }
    } else {
      COSBase font = entries.getItem(name);
      if (dictionary(font) == null) {
        return;
      }
      if (!(font instanceof COSObject)
          && !materializedDirectFonts.computeIfAbsent(resources, r -> new HashSet<>()).add(name)) {
        return;
      }
      checkFont(font);
    }
  }

  /**
   * Checks the entries of {@code resources} that {@code drawn} names: forms (recursively), fonts, and ExtGState fonts.
   */
  private void walkResources(COSDictionary resources, int level, DrawnResources drawn) throws IOException {
    walkDrawnForms(resources, level, drawn);
    checkDrawnFonts(resources, drawn);
    checkDrawnExtGStateFonts(resources, drawn);
  }

  /** Visits the drawn form XObjects of {@code resources}, while the form nesting is under its limit. */
  private void walkDrawnForms(COSDictionary resources, int level, DrawnResources drawn) throws IOException {
    COSDictionary xobjects = dictionary(resources.getItem(COSName.XOBJECT));
    if (xobjects != null && level < MAX_FORM_LEVEL) {
      for (COSName name : drawnEntries(xobjects, DrawnResources.Category.XOBJECT, drawn)) {
        COSBase item = xobjects.getItem(name);
        COSBase xobject = resolve(item);
        if (xobject instanceof COSStream && COSName.FORM.equals(((COSStream) xobject).getCOSName(COSName.SUBTYPE))) {
          visitForm(item, (COSStream) xobject, level + 1, resources);
        }
      }
    }
  }

  /** Checks the drawn fonts of {@code resources}. */
  private void checkDrawnFonts(COSDictionary resources, DrawnResources drawn) throws IOException {
    COSDictionary fonts = dictionary(resources.getItem(COSName.FONT));
    if (fonts != null) {
      for (COSName name : drawnEntries(fonts, DrawnResources.Category.FONT, drawn)) {
        checkFont(fonts.getItem(name));
      }
    }
  }

  /** Checks the font named by each drawn ExtGState of {@code resources}. */
  private void checkDrawnExtGStateFonts(COSDictionary resources, DrawnResources drawn) throws IOException {
    COSDictionary states = dictionary(resources.getItem(COSName.EXT_G_STATE));
    if (states != null) {
      for (COSName name : drawnEntries(states, DrawnResources.Category.EXT_G_STATE, drawn)) {
        COSDictionary state = dictionary(states.getItem(name));
        COSBase font = state == null ? null : resolve(state.getItem(COSName.FONT));
        if (font instanceof COSArray && ((COSArray) font).size() > 0) {
          checkFont(((COSArray) font).get(0));
        }
      }
    }
  }

  /**
   * The entries of {@code entries} that {@code drawn} names in {@code category}, looked up by name, so the cost of a walk
   * follows what the content draws, not the dictionary's size (a large dictionary shared by many pages is not scanned on
   * each); every entry when the record means every resource.
   */
  private Iterable<COSName> drawnEntries(COSDictionary entries, DrawnResources.Category category, DrawnResources drawn) {
    if (drawn.drawsEverything()) {
      resourceEntriesVisited += entries.size();
      return entries.keySet();
    }
    List<COSName> found = new ArrayList<>();
    // looked up by the COSName itself: its bytes, as PDFBox resolves it (a name need not be UTF-8)
    for (COSName name : drawn.names(category)) {
      if (entries.containsKey(name)) {
        found.add(name);
      }
    }
    resourceEntriesVisited += drawn.names(category).size();
    return found;
  }

  /**
   * @param callerResources the resources the {@code Do} was resolved in, which the form's names resolve in too when it
   *                        has no /Resources key (a /Resources key that is not a dictionary resolves nothing)
   */
  private void visitForm(COSBase item, COSStream form, int level, COSDictionary callerResources) throws IOException {
    COSDictionary own = dictionary(form.getItem(COSName.RESOURCES));
    if (own == null && form.containsKey(COSName.RESOURCES)) {
      // PDFormXObject#getResources: a /Resources that is not a dictionary gives empty resources, so nothing resolves
      checkStream(item, form, false, false, Kind.CONTENT);
      return;
    }
    COSDictionary resources = own != null ? own : callerResources;
    Map<COSDictionary, Integer> levels = formLevels.computeIfAbsent(form, f -> new IdentityHashMap<>());
    Integer seen = levels.get(resources);
    if (seen != null && seen <= level) {
      return;                                          // already expanded in these resources at this level or a shallower one
    }
    levels.put(resources, level);
    DrawnResources drawn = checkStream(item, form, false, false, Kind.CONTENT);
    if (drawn != null) {
      walkResources(resources, level, drawn);
    }
  }

  private void warnSkippedContent(int pageNumber) {
    if (skippedContent == 0) {
      return;
    }
    warn.accept((pageNumber > 0 ? "PDF page " + pageNumber : "A PDF page") + ": skipped " + skippedContent + " content stream(s) that cannot be decoded "
        + skippedContentNames + (skippedContent > skippedContentNames.size() ? " ..." : "")
        + "; text they draw is missing from this page's extraction");
  }

  private void checkFont(COSBase fontItem) throws IOException {
    checkFont(fontItem, true);
  }

  private void checkFont(COSBase fontItem, boolean cacheable) throws IOException {
    COSDictionary font = dictionary(fontItem);
    if (font == null) {
      return;
    }
    COSObjectKey key = fontItem instanceof COSObject ? fontItem.getKey() : null;
    if (cacheable && key != null && !chargedFontObjects.add(key)) {
      return;
    }
    if (++fontVisits > MAX_STREAMS_PER_PAGE) {
      throw new PageFailure(PdfFailureCode.PAGE_ERROR, "more than " + MAX_STREAMS_PER_PAGE
          + " font dictionary visits on one page");
    }
    // A distinct dictionary may create a distinct font copy even when it references an already-checked stream.
    chargedFontStreams = Collections.newSetFromMap(new IdentityHashMap<>());
    try {
      checkFontFiles(dictionary(font.getItem(COSName.FONT_DESC)));
      checkOptionalStream(font.getItem(COSName.TO_UNICODE), Kind.CMAP);
      checkOptionalStream(font.getItem(COSName.ENCODING), Kind.CMAP);
      checkDescendantFonts(resolve(font.getItem(COSName.DESCENDANT_FONTS)));
      checkCharProcs(dictionary(font.getItem(COSName.CHAR_PROCS)));
    } finally {
      chargedFontStreams = null;
    }
  }

  /** Checks the font files and CIDToGIDMap of each descendant font of a composite font. */
  private void checkDescendantFonts(COSBase descendants) throws IOException {
    if (!(descendants instanceof COSArray)) {
      return;
    }
    COSArray array = (COSArray) descendants;
    for (int i = 0; i < array.size(); i++) {
      COSDictionary descendant = dictionary(array.get(i));
      if (descendant != null) {
        checkFontFiles(dictionary(descendant.getItem(COSName.FONT_DESC)));
        checkOptionalStream(descendant.getItem(COSName.CID_TO_GID_MAP), Kind.OPAQUE_COPIED);
      }
    }
  }

  /** Checks a Type3 font's CharProcs against the per-font limit, then each CharProc stream. */
  private void checkCharProcs(COSDictionary charProcs) throws IOException {
    if (charProcs == null) {
      return;
    }
    if (charProcs.size() > MAX_CHAR_PROCS_PER_FONT) {
      throw new PageFailure(PdfFailureCode.PAGE_ERROR, "Type3 font with " + charProcs.size() + " CharProcs, more than "
          + MAX_CHAR_PROCS_PER_FONT);
    }
    for (COSName name : charProcs.keySet()) {
      COSBase item = charProcs.getItem(name);
      COSBase proc = resolve(item);
      if (proc instanceof COSStream) {
        checkStream(item, (COSStream) proc, true, true, Kind.CONTENT);
      }
    }
  }

  private void checkFontFiles(COSDictionary descriptor) throws IOException {
    if (descriptor != null) {
      checkOptionalStream(descriptor.getItem(COSName.FONT_FILE), Kind.TYPE1);
      checkOptionalStream(descriptor.getItem(COSName.FONT_FILE2), Kind.OPAQUE_COPIED);
      checkOptionalStream(descriptor.getItem(COSName.FONT_FILE3), Kind.OPAQUE_COPIED);
    }
  }

  private void checkOptionalStream(COSBase item, Kind kind) throws IOException {
    COSBase resolved = resolve(item);
    if (resolved instanceof COSStream) {
      checkStream(item, (COSStream) resolved, false, true, kind);
    }
  }

  /**
   * @return for a content stream that is not part of the page's /Contents, what it draws (null when it is skipped as
   *     undecodable, as PDFBox draws nothing from it); null for other kinds and for the page's /Contents, whose drawn
   *     resources the shared page scanner collects
   */
  private DrawnResources checkStream(COSBase item, COSStream stream, boolean charProc, boolean fontSide, Kind kind)
      throws IOException {
    COSObjectKey key = item instanceof COSObject ? item.getKey() : stream.getKey();
    boolean countFont = fontSide && kind != Kind.CONTENT;
    boolean pageContent = kind == Kind.CONTENT && pageOperands != null;
    boolean streamingContent = pageContent && stream == streamingFlateContent;
    Set<COSObjectKey> streamFailures = streamingContent ? streamingContentFailed : failed;
    Set<COSObjectKey> streamUndecodable = streamingContent ? streamingContentUndecodable : undecodable;
    if (pageContent) {
      pageContentStreams++;
    }
    checkNotFailedBefore(key, streamFailures);
    if (key != null && streamUndecodable.contains(key)) {
      noteSkippedAgain(kind, charProc, key, pageContent);
      return null;                                     // skipped again, as on the page that first met it
    }
    EnumMap<Kind, Long> kinds = key != null ? passed.get(key) : null;
    boolean chargeDecoded = chargeFontVisit(countFont, stream, kinds);
    if (!pageContent && kinds != null && kinds.containsKey(kind)) {
      return passedBefore(key, stream, charProc, kind);
    }
    countVisitedStream(charProc);
    OptionalLong length = decodeChecked(stream, key, kind, charProc, pageContent, chargeDecoded,
        streamFailures, streamUndecodable);
    if (!length.isPresent()) {
      return null;                                     // skipped, as PDFBox skips it
    }
    chargePageContentBytes(pageContent, stream, key, length.getAsLong());
    rememberPassed(key, pageContent, kind, length.getAsLong());
    if (kind != Kind.CONTENT || pageContent) {
      return null;
    }
    return contentParsed(key, stream, charProc);
  }

  /** Refuses a stream that already failed the decode guard in this document. */
  private void checkNotFailedBefore(COSObjectKey key, Set<COSObjectKey> streamFailures) {
    if (key != null && streamFailures.contains(key)) {
      throw new PageFailure(PdfFailureCode.DECODE_LIMIT, STREAM + key + " already failed the decode guard");
    }
  }

  /** Notes a stream already known to be undecodable as skipped on this page. */
  private void noteSkippedAgain(Kind kind, boolean charProc, COSObjectKey key, boolean pageContent) {
    noteSkippedContent(kind, charProc, key, null);
    if (pageContent) {
      undecodableContentStreams++;
    }
  }

  /**
   * Charges the font budget on a font stream's first visit on this page: its decoded size when it passed before.
   *
   * @return whether the decode must charge the font budget instead (a stream not decoded before)
   */
  private boolean chargeFontVisit(boolean countFont, COSStream stream, EnumMap<Kind, Long> kinds) {
    if (!countFont || !firstFontStreamVisit(stream)) {
      return false;
    }
    if (kinds == null) {
      return true;
    }
    fontBytes.add(kinds.values().iterator().next());
    return false;
  }

  /** The result for a stream that passed this check as {@code kind} before. */
  private DrawnResources passedBefore(COSObjectKey key, COSStream stream, boolean charProc, Kind kind) {
    return kind == Kind.CONTENT ? contentParsed(key, stream, charProc) : null;
  }

  /** Counts a stream against the per-page stream limit; CharProcs are not counted. */
  private void countVisitedStream(boolean charProc) {
    if (!charProc && ++visited > MAX_STREAMS_PER_PAGE) {
      throw new PageFailure(PdfFailureCode.PAGE_ERROR, "more than " + MAX_STREAMS_PER_PAGE + " streams to check on one page");
    }
  }

  /**
   * Decodes the stream within the decode budget, scanning its structure.
   *
   * @return its decoded length; empty when it is skipped as undecodable
   */
  private OptionalLong decodeChecked(COSStream stream, COSObjectKey key, Kind kind, boolean charProc, boolean pageContent,
      boolean chargeDecoded, Set<COSObjectKey> streamFailures, Set<COSObjectKey> streamUndecodable) throws IOException {
    StreamStructureScanner.Scanner scanner = pageContent ? pageOperands : scanner(kind, key);
    StreamStructureScanner.Operands beforeStream = pageContent ? pageOperands.copy() : null;
    File spool = kind == Kind.TYPE1 ? ScratchFiles.createTempFile(tempDir.toPath(), "guard-type1-", ".bin") : null;
    try {
      long length = decodeInto(stream, key, kind, scanner, spool, chargeDecoded);
      if (pageContent && pageContentsArray) {
        pageOperands.write('\n');                    // PDPage.getContentsForRandomAccess puts a newline after each stream
      }
      return OptionalLong.of(length);
    } catch (PageFailure e) {
      rememberFailed(key, e, pageContent, streamFailures);
      throw e;
    } catch (IOException e) {
      skipUndecodable(e, kind, charProc, key, pageContent, beforeStream, streamUndecodable);
      return OptionalLong.empty();
    } finally {
      delete(spool);
    }
  }

  /** Decodes into the scanner, or for a Type1 font file into the spool file, which is then checked. */
  private long decodeInto(COSStream stream, COSObjectKey key, Kind kind, StreamStructureScanner.Scanner scanner,
      File spool, boolean chargeDecoded) throws IOException {
    if (spool == null) {
      return decodeCounting(stream, key, scanner, kind == Kind.CONTENT, chargeDecoded);
    }
    long length;
    try (OutputStream out = new BufferedOutputStream(ScratchFiles.newOutputStream(spool.toPath()), BUFFER)) {
      length = decodeCounting(stream, key, out, false, chargeDecoded);
    }
    Type1FontCheck.check(spool, stream.getInt(COSName.LENGTH1), stream.getInt(COSName.LENGTH2), STREAM + key);
    return length;
  }

  /** Remembers a decode-limit refusal that is a property of the stream itself as failed. */
  private void rememberFailed(COSObjectKey key, PageFailure e, boolean pageContent, Set<COSObjectKey> streamFailures) {
    if (key != null && e.code() == PdfFailureCode.DECODE_LIMIT && !(e instanceof FontBudgetRefusal)
        && !(pageContent && e instanceof StreamStructureScanner.Refusal)) {
      streamFailures.add(key);                         // a sequence-dependent refusal is not a property of the stream
    }
  }

  /** Records a stream that cannot be decoded as skipped; a scratch disk failure fails the round instead. */
  private void skipUndecodable(IOException e, Kind kind, boolean charProc, COSObjectKey key, boolean pageContent,
      StreamStructureScanner.Operands beforeStream, Set<COSObjectKey> streamUndecodable) {
    RuntimeException round = PrepFailures.roundFailure(e);
    if (round != null) {
      throw round;                                     // a decoder wrapped a scratch disk failure: never the stream's
    }
    if (key != null) {
      streamUndecodable.add(key);
    }
    if (pageContent) {
      undecodableContentStreams++;
      pageOperands = beforeStream;                     // PDFBox parses the page without any byte of this stream
    }
    noteSkippedContent(kind, charProc, key, e);
  }

  /** Charges a filtered page /Contents stream against the page's decoded-bytes budget. */
  private void chargePageContentBytes(boolean pageContent, COSStream stream, COSObjectKey key, long length)
      throws IOException {
    if (!pageContent || filters(stream, key).isEmpty()) {
      return;
    }
    pageContentBytes += length;                        // after the catch: a sum refusal is not a property of the stream
    if (pageContentBytes > maxDecodedBytes) {
      throw new PageFailure(PdfFailureCode.DECODE_LIMIT, "the filtered /Contents streams of the page decode to more than "
          + maxDecodedBytes + " bytes together");
    }
  }

  /** Remembers the decoded length of an indirect stream that passed as {@code kind}, outside the page /Contents. */
  private void rememberPassed(COSObjectKey key, boolean pageContent, Kind kind, long length) {
    if (key != null && !pageContent) {
      passed.computeIfAbsent(key, k -> new EnumMap<>(Kind.class)).put(kind, length);
    }
  }

  /**
   * Whether PDPage#getContentsForStreamParsing inflates this page's /Contents itself, ignoring /DecodeParms: one stream
   * whose /Filter is exactly /FlateDecode. The guard decoded that stream with its predictor, so PDFBox may read more (or
   * other) bytes than were checked; the parse gets its own byte budget then.
   */
  private static boolean readsUnpredictedFlate(PDPage page) {
    COSStream contents = page.getCOSObject().getCOSStream(COSName.CONTENTS);
    return contents != null && COSName.FLATE_DECODE.equals(contents.getFilters());
  }

  /**
   * The budgeted PDFBox parse of a form or a Type3 CharProc, which text extraction parses too (a form when drawn; a
   * CharProc for a glyph's width when /Widths lacks it, and for glyph boxes when /FontBBox is empty). Each indirect
   * stream is parsed once per document: a pass is remembered, a refusal is cached as failed like a stream past the
   * decoded-size cap. A form's drawn names are kept within {@link #MAX_CACHED_DRAWN_NAMES}; a form parsed before whose
   * names were not kept draws every resource.
   *
   * @return what a form draws; null for a CharProc, whose names are never resolved here
   */
  private DrawnResources contentParsed(COSObjectKey key, COSStream stream, boolean charProc) {
    if (key != null && parsedWithinBudgets.contains(key)) {
      DrawnResources cached = drawnByStream.get(key);
      if (charProc) {
        return null;
      }
      return cached != null ? cached : DrawnResources.everything();
    }
    DrawnResources drawn;
    try {
      drawn = drawnBy(new FormContent(stream), Long.MAX_VALUE);
    } catch (PageFailure e) {
      if (key != null) {
        passed.remove(key);
        failed.add(key);
      }
      throw e;
    }
    if (key == null) {
      return charProc ? null : drawn;
    }
    parsedWithinBudgets.add(key);
    if (charProc) {
      return null;
    }
    long cost = Math.max(1, drawn.size());             // an entry with no names still holds memory
    if (cachedDrawnNames + cost <= MAX_CACHED_DRAWN_NAMES) {
      cachedDrawnNames += cost;
      drawnByStream.put(key, drawn);
    }
    return drawn;
  }

  /**
   * The resources {@code content} hands to Do, Tf and gs, read by PDFBox's own parser; every resource when it cannot be
   * parsed. A budget refusal fails the page; a scratch-disk failure is not the page's and fails the round, as in
   * {@link #checkStream}. Called only after this guard has checked the streams it parses.
   *
   * @param maxContentBytes bytes the whole content may be read from; {@code Long.MAX_VALUE} where PDFBox reads exactly
   *                        the bytes the guard decoded
   */
  private DrawnResources drawnBy(PDContentStream content, long maxContentBytes) {
    try (BoundedContentParser parser = BoundedContentParser.of(content, StreamStructureScanner.MAX_OPERANDS,
        maxDecodedBytes, maxContentBytes)) {
      return DrawnResources.collect(parser);
    } catch (PageFailure | PrepPlatformException e) {
      throw e;                                         // a budget refusal; the executor's scratch disk, never the page
    } catch (IOException | RuntimeException e) {
      RuntimeException round = PrepFailures.roundFailure(e);
      if (round != null) {
        throw round;                                   // a scratch-disk failure, or an unchecked wrapper around one
      }
      return DrawnResources.everything();
    }
  }

  /**
   * A form's content as PDFormXObject#getContentsForRandomAccess reads it (a view of the stream), without constructing a
   * PDFormXObject, whose constructor writes /Type and /Subtype into the form dictionary.
   */
  private static final class FormContent implements PDContentStream {
    private final COSStream stream;

    FormContent(COSStream stream) {
      this.stream = stream;
    }

    @Override
    public InputStream getContents() throws IOException {
      return new RandomAccessInputStream(getContentsForRandomAccess());
    }

    @Override
    public RandomAccessRead getContentsForRandomAccess() throws IOException {
      return stream.createView();
    }

    @Override
    public PDResources getResources() {
      return null;                                     // the parser reads no resources
    }

    @Override
    public PDRectangle getBBox() {
      return null;
    }

    @Override
    public Matrix getMatrix() {
      return new Matrix();
    }
  }

  /** A /Contents stream or a drawn form PDFBox leaves out of the page; CharProcs and font streams lose no page text. */
  private void noteSkippedContent(Kind kind, boolean charProc, COSObjectKey key, IOException cause) {
    if (kind != Kind.CONTENT || charProc || (key != null && !skippedContentKeys.add(key))) {
      return;                                          // a stream listed or drawn again on this page is named once
    }
    if (++skippedContent <= MAX_LOGGED_SKIPS) {
      String error = cause == null ? "cannot be decoded" : cause.getClass().getSimpleName() + ": " + cause.getMessage();
      if (error.length() > MAX_LOGGED_ERROR_CHARS) {
        error = error.substring(0, MAX_LOGGED_ERROR_CHARS) + "...";
      }
      skippedContentNames.add((key == null ? "a direct stream" : STREAM + key) + " (" + error + ")");
    }
  }

  private boolean firstFontStreamVisit(COSStream stream) {
    if (!chargedFontStreams.add(stream)) {
      return false;
    }
    if (chargedFontStreams.size() > MAX_STREAMS_PER_PAGE) {
      throw new PageFailure(PdfFailureCode.PAGE_ERROR, "more than " + MAX_STREAMS_PER_PAGE
          + " distinct font streams in one font dictionary");
    }
    return true;
  }

  private StreamStructureScanner.Scanner scanner(Kind kind, COSObjectKey key) {
    String where = STREAM + key;
    switch (kind) {
      case CONTENT:
        return new StreamStructureScanner.Operands(where, maxDecodedBytes);
      case CMAP:
        return new StreamStructureScanner.CMapRanges(where);
      case TYPE1: // spooled, then checked by Type1FontCheck
      case OPAQUE_COPIED:
        return null;
      default:
        throw new UnsupportedOperationException("no pre-scan for stream kind " + kind);
    }
  }

  /**
   * Decodes {@code stream} into counting sinks, following PDFBox's filter-list semantics (Filter.java:241-310); the last
   * stage feeds {@code scanner} when there is one.
   *
   * @param viewWhenUnfiltered whether PDFBox reads this stream unfiltered as a view on the staged file (content streams),
   * so its raw bytes are scanned without the decoded-size cap on the whole stream; font files, CMaps and CIDToGIDMaps are
   *                           copied onto the heap whole by PDFBox or fontbox, so they keep the cap filtered or not
   *
   */
  private long decodeCounting(COSStream stream, COSObjectKey key, OutputStream scanner, boolean viewWhenUnfiltered,
                              boolean chargeFont)
      throws IOException {
    List<Filter> filters = filters(stream, key);
    decodedStreams++;
    boolean streamingFlate = pageOperands != null && stream == streamingFlateContent;
    if (filters.isEmpty() || streamingFlate) {
      // Single scalar-Flate page content streams inflation without Predictor; arrays use Filter.decode below.
      return copyRaw(stream, key, scanner, viewWhenUnfiltered && filters.isEmpty() ? Long.MAX_VALUE : maxDecodedBytes,
          chargeFont, streamingFlate);
    }
    for (int i = 0; i < filters.size(); i++) {
      refuseHugePredictorRow(stream, names(stream).get(i), i, key);
    }
    return decodeChain(stream, key, scanner, filters, chargeFont);
  }

  /** Copies raw or scalar-Flate-streamed bytes to {@code scanner} within {@code limit}; returns the count. */
  private long copyRaw(COSStream stream, COSObjectKey key, OutputStream scanner, long limit, boolean chargeFont, boolean streamingFlate)
      throws IOException {
    try (InputStream raw = stream.createRawInputStream();
         InputStream in = streamingFlate ? new FlateFilterDecoderStream(raw) : raw;
         CountingSink out = new CountingSink(scanner, limit, key, chargeFont ? fontBytes : null)) {
      byte[] buffer = new byte[BUFFER];
      for (int n = in.read(buffer); n >= 0; n = in.read(buffer)) {
        out.write(buffer, 0, n);
      }
      return out.count;
    }
  }

  /** Decodes {@code filters} in turn, each into a scratch file and the last into {@code scanner}; returns the last's count. */
  private long decodeChain(COSStream stream, COSObjectKey key, OutputStream scanner, List<Filter> filters, boolean chargeFont)
      throws IOException {
    File previous = null;
    File next = null;
    long finalLength = 0;
    try {
      for (int i = 0; i < filters.size(); i++) {
        boolean last = i == filters.size() - 1;
        next = last ? null : ScratchFiles.createTempFile(tempDir.toPath(), "guard-", ".bin");
        try (InputStream in = previous == null ? stream.createRawInputStream()
                : new BufferedInputStream(ScratchFiles.newInputStream(previous.toPath()), BUFFER);
             CountingSink out = new CountingSink(next == null ? scanner
                 : new BufferedOutputStream(ScratchFiles.newOutputStream(next.toPath()), BUFFER), maxDecodedBytes, key,
                 last && chargeFont ? fontBytes : null)) {
          filters.get(i).decode(in, out, stream, i);
          if (last) {
            finalLength = out.count;
          }
        }
        delete(previous);
        previous = next;
        next = null;
      }
    } finally {
      delete(previous);
      delete(next);                                    // the intermediate of a filter that failed
    }
    return finalLength;
  }

  /** The filter names in the order {@link #filters} keeps them (duplicates reduced). */
  private static List<COSName> names(COSStream stream) {
    COSBase names = stream.getFilters();
    List<COSName> result = new ArrayList<>();
    if (names instanceof COSName) {
      result.add((COSName) names);
    } else if (names instanceof COSArray) {
      COSArray array = (COSArray) names;
      List<Filter> seen = new ArrayList<>();
      for (int i = 0; i < array.size(); i++) {
        COSName name = (COSName) array.get(i);
        try {
          Filter filter = FilterFactory.INSTANCE.getFilter(name);
          if (!seen.contains(filter)) {
            seen.add(filter);
            result.add(name);
          }
        } catch (IOException e) {
          result.add(name);
        }
      }
    }
    return result;
  }

  /**
   * Refuses, without decoding, a Flate/LZW stage whose predictor row (computed as PDFBox's Predictor computes it, in int
   * arithmetic) is negative or above {@link #MAX_PREDICTOR_ROW_BYTES}.
   */
  private static void refuseHugePredictorRow(COSStream stream, COSName filter, int index, COSObjectKey key) {
    if (!PREDICTOR_FILTERS.contains(filter)) {
      return;
    }
    COSDictionary params = decodeParams(stream, index);
    if (params.getInt(COSName.PREDICTOR, 1) <= 1) {
      return;
    }
    int colors = Math.min(params.getInt(COSName.COLORS, 1), 32);
    int bitsPerComponent = params.getInt(COSName.BITS_PER_COMPONENT, 8);
    int columns = params.getInt(COSName.COLUMNS, 1);
    int row = (columns * (colors * bitsPerComponent) + 7) / 8;      // Predictor.calculateRowLength
    long exact = ((long) columns * colors * bitsPerComponent + 7) / 8;
    if (row < 0 || row > MAX_PREDICTOR_ROW_BYTES || exact > MAX_PREDICTOR_ROW_BYTES) {
      throw new PageFailure(PdfFailureCode.DECODE_LIMIT, "predictor row of stream " + key + " is " + exact
          + " bytes, above " + MAX_PREDICTOR_ROW_BYTES + " (refused without decoding)");
    }
  }

  /** The DecodeParms of filter {@code index}, resolved as PDFBox's Filter.getDecodeParams resolves them. */
  private static COSDictionary decodeParams(COSStream stream, int index) {
    COSBase filter = stream.getDictionaryObject(COSName.F, COSName.FILTER);
    COSBase params = stream.getDictionaryObject(COSName.DP, COSName.DECODE_PARMS);
    if (filter instanceof COSName && params instanceof COSDictionary) {
      return (COSDictionary) params;
    }
    if (filter instanceof COSArray && params instanceof COSArray && index < ((COSArray) params).size()) {
      COSBase atIndex = ((COSArray) params).getObject(index);
      if (atIndex instanceof COSDictionary) {
        return (COSDictionary) atIndex;
      }
    }
    return new COSDictionary();
  }

  /** The stream's filters with duplicates reduced as PDFBox reduces them. */
  private static List<Filter> filters(COSStream stream, COSObjectKey key) throws IOException {
    COSBase names = stream.getFilters();
    List<Filter> filters = new ArrayList<>();
    if (names instanceof COSName) {
      refuseImageCodec((COSName) names, key);
      filters.add(FilterFactory.INSTANCE.getFilter((COSName) names));
    } else if (names instanceof COSArray) {
      COSArray array = (COSArray) names;
      for (int i = 0; i < array.size(); i++) {
        COSBase name = array.get(i);
        if (!(name instanceof COSName)) {
          throw new IOException("Forbidden type in filter array: " + (name == null ? "null" : name.getClass().getName()));
        }
        refuseImageCodec((COSName) name, key);
        Filter filter = FilterFactory.INSTANCE.getFilter((COSName) name);
        if (!filters.contains(filter)) {
          filters.add(filter);
        }
      }
    }
    return filters;
  }

  private static void refuseImageCodec(COSName filter, COSObjectKey key) {
    if (IMAGE_CODECS.contains(filter)) {
      throw new PageFailure(PdfFailureCode.DECODE_LIMIT, "image codec " + filter.getName() + " in a text-extraction stream "
          + key + " (refused without decoding)");
    }
  }

  private static COSBase resolve(COSBase base) {
    return base instanceof COSObject ? ((COSObject) base).getObject() : base;
  }

  private static COSDictionary dictionary(COSBase base) {
    COSBase resolved = resolve(base);
    return resolved instanceof COSDictionary ? (COSDictionary) resolved : null;
  }

  private static void delete(File file) {
    if (file == null) {
      return;
    }
    try {
      Files.deleteIfExists(file.toPath());
    } catch (IOException e) {
      if (file.exists()) {
        file.deleteOnExit();
      }
    }
  }

  /** Page-local admission counter; a refusal must never be cached as a stream failure. */
  static final class FontBytes {
    private final long budget;
    private long sum;

    FontBytes(long budget) {
      this.budget = budget;
    }

    void add(long bytes) {
      if (bytes > budget - sum) {
        String attempted = bytes > Long.MAX_VALUE - sum ? "overflow (> Long.MAX_VALUE)" : Long.toString(sum + bytes);
        throw new FontBudgetRefusal("page font bytes budget=" + budget + " sum=" + attempted);
      }
      sum += bytes;
    }
  }

  private static final class FontBudgetRefusal extends PageFailure {
    FontBudgetRefusal(String message) {
      super(PdfFailureCode.DECODE_LIMIT, message);
    }
  }

  /** Counts decoded bytes, optionally passing them on; throws once more than the cap have arrived. */
  private static final class CountingSink extends OutputStream {
    private final OutputStream next;
    private final long cap;
    private final COSObjectKey key;
    private long count;
    private final FontBytes fontBytes;

    CountingSink(OutputStream next, long cap, COSObjectKey key, FontBytes fontBytes) {
      this.next = next;
      this.cap = cap;
      this.key = key;
      this.fontBytes = fontBytes;
    }

    @Override
    public void write(int b) throws IOException {
      add(1);
      if (next != null) {
        next.write(b);
      }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      add(len);
      if (next != null) {
        next.write(b, off, len);
      }
    }

    private void add(long n) {
      if (n > cap - count) {
        throw new PageFailure(PdfFailureCode.DECODE_LIMIT, STREAM + key + " decodes to more than " + cap + " bytes");
      }
      if (fontBytes != null) {
        fontBytes.add(n);
      }
      count += n;
    }

    @Override
    public void flush() throws IOException {
      if (next != null) {
        next.flush();
      }
    }

    @Override
    public void close() throws IOException {
      if (next != null) {
        next.close();
      }
    }
  }
}
