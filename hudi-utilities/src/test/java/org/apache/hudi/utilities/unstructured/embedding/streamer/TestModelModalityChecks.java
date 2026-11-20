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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.compute.KnownEmbeddingModels;
import org.apache.hudi.utilities.unstructured.embedding.compute.TestImageRowRouting;
import org.apache.hudi.utilities.unstructured.embedding.compute.bedrock.BedrockCohereEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessorRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The start checks of a model's declared modalities ({@code hoodie.unstructured.embedding.model.<alias>.modalities}): the key's
 * values, the refusals of an unset key, of a modality the model's client has no request format for and of one beyond a known
 * model, and the prepared content against the declaration with the error table off (refused) and on (started).
 */
public class TestModelModalityChecks {

  private static final String KEY = "hoodie.unstructured.embedding.model.m1.modalities";

  @Test
  public void valuesAreASubsetOfTextAndImageInAnyCaseAndOrder() {
    assertEquals(set("text"), model("dummy", "m", "text").getModalities());
    assertEquals(set("image"), model("dummy", "m", " IMAGE ").getModalities());
    assertEquals(set("image", "text"), model("dummy", "m", "Image,text").getModalities());
    assertEquals(set("image", "text"), model("dummy", "m", "text, image, text,").getModalities());
    assertEquals(Collections.emptySet(), model("dummy", "m", null).getModalities(), "unset declares nothing");
  }

  @Test
  public void invalidValueIsRefusedNamingTheKey() {
    IllegalArgumentException video = assertThrows(IllegalArgumentException.class, () -> model("dummy", "m", "text,video"));
    assertEquals("Config " + KEY + "=text,video names modality 'video'; the modalities are text and image, e.g. " + KEY
        + "=text,image", video.getMessage());
    IllegalArgumentException empty = assertThrows(IllegalArgumentException.class, () -> model("dummy", "m", " , "));
    assertEquals("Config " + KEY + "= ,  lists no modality; set it to text, image or text,image", empty.getMessage());
  }

  @Test
  public void unsetKeyIsRefusedNamingIt() {
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> ModelModalityChecks.checkDeclaration(model("dummy", "m", null)));
    assertEquals("Missing required config " + KEY + ": declare the modalities model m1 embeds: text, image or text,image. "
        + "Nothing was written", refused.getMessage());
  }

  /** Declarations every client can send, within the known models. */
  @ParameterizedTest(name = "{0} {1} {2}")
  @CsvSource(delimiter = '|', value = {
      "dummy|m|text",
      "dummy-multimodal|m|text,image",
      "dummy-multimodal|m|image",
      "bedrock|amazon.titan-embed-text-v2:0|text",
      "bedrock|cohere.embed-v4:0|text,image",
      "bedrock|us.cohere.embed-v4:0|image",
      "bedrock|amazon.titan-embed-image-v1|text,image",
      "vertex-multimodal|multimodalembedding@001|text,image",
      "openai|text-embedding-3-small|text",
      "azure-openai|text-embedding-3-large|text",
      "hf-tei|BAAI/bge-small-en-v1.5|text",
      "jina|jina-embeddings-v3|text"})
  public void declarationTheClientCanSendStarts(String provider, String name, String modalities) {
    ModelModalityChecks.checkDeclaration(model(provider, name, modalities));
  }

  /** A modality the provider's client has no request format for is refused, naming the provider and the model. */
  @ParameterizedTest(name = "{0} {1}")
  @CsvSource(delimiter = '|', value = {
      "dummy|m|org.apache.hudi.utilities.unstructured.embedding.compute.DummyEmbeddingClient|[text]|",
      "bedrock|amazon.titan-embed-text-v2:0|org.apache.hudi.utilities.unstructured.embedding.compute.bedrock."
          + "BedrockTitanTextEmbeddingClient|[text]|",
      "openai|text-embedding-3-small|org.apache.hudi.utilities.unstructured.embedding.compute.openai."
          + "OpenAiCompatibleEmbeddingClient|[text]| (the OpenAI embeddings API takes no image input)",
      "azure-openai|text-embedding-3-small|org.apache.hudi.utilities.unstructured.embedding.compute.openai."
          + "OpenAiCompatibleEmbeddingClient|[text]| (the OpenAI embeddings API takes no image input)",
      "hf-tei|clip|org.apache.hudi.utilities.unstructured.embedding.compute.openai.OpenAiCompatibleEmbeddingClient|[text]"
          + "| (image input for OpenAI-compatible servers is not supported)"})
  public void imageDeclarationTheClientCannotSendIsRefused(String provider, String name, String client, String supported,
                                                              String reason) {
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> ModelModalityChecks.checkDeclaration(model(provider, name, "text,image")));
    assertEquals("Config " + KEY + "=image,text declares [image], which provider " + provider + " has no request format for "
        + "with model " + name + (reason == null ? "" : " " + reason) + "; its client " + client + " embeds " + supported
        + ". Nothing was written", refused.getMessage());
  }

  @Test
  public void declarationBeyondAKnownModelIsRefused() {
    // a client that sends images, serving a model known to embed text only
    TypedProperties props = props("dummy", "amazon.titan-embed-text-v2:0", "text,image");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX,
        TestImageRowRouting.ImageClient.class.getName());
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> ModelModalityChecks.checkDeclaration(EmbeddingTableConfig.from(props).model("m1")));
    assertEquals("Config " + KEY + "=image,text declares [image], but model amazon.titan-embed-text-v2:0 (provider dummy) "
        + "embeds only [text]. Nothing was written", refused.getMessage());
  }

  @Test
  public void modelOutsideTheTableIsTrustedAsDeclared() {
    TypedProperties props = props("bedrock", "arn:aws:bedrock:us-east-1:123456789012:provisioned-model/abc", "text,image");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX,
        BedrockCohereEmbeddingClient.class.getName());
    ModelModalityChecks.checkDeclaration(EmbeddingTableConfig.from(props).model("m1"));
  }

  @ParameterizedTest(name = "{0}")
  @CsvSource(delimiter = '|', value = {
      "amazon.titan-embed-text-v2:0|text",
      "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v2:0|text",
      "cohere.embed-v4:0|image,text",
      "eu.cohere.embed-v4:0|image,text",
      "amazon.titan-embed-image-v1|image,text",
      "multimodalembedding@001|image,text",
      "text-embedding-3-small|text",
      "TEXT-EMBEDDING-3-LARGE|text"})
  public void knownModels(String name, String modalities) {
    assertEquals(set(modalities.split(",")), KnownEmbeddingModels.modalities(name).get());
  }

  @Test
  public void unknownModelsHaveNoEntry() {
    for (String name : Arrays.asList("cohere.embed-english-v3", "multimodalembedding", "text-embedding-ada-002",
        "arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/xyz", "BAAI/bge-small-en-v1.5")) {
      assertFalse(KnownEmbeddingModels.modalities(name).isPresent(), name);
    }
    assertFalse(KnownEmbeddingModels.modalities(null).isPresent());
  }

  @Test
  public void preparedModalitiesFollowTheExtensionsAndTheAudioSetting() {
    assertEquals(map("text", "pdf"), prepared(props("dummy", "m", "text", "pdf")));
    assertEquals(map("image", "jpg,png", "text", "pdf"), prepared(props("dummy", "m", "text", "PDF,png,jpg")));
    assertEquals(map("image", "mp4"), prepared(props("dummy", "m", "text", "mp4")));
    TypedProperties speech = props("dummy", "m", "text", "mp4");
    speech.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    assertEquals(map("image", "mp4", "text", "mp4"), prepared(speech), "a video's speech adds text rows");
    Map<String, Set<String>> all = prepared(props("dummy", "m", "text", null));
    assertEquals(set("image", "text"), all.keySet(), "unset: every mapped extension is prepared");
    assertEquals(set("pdf"), all.get("text"));
    assertEquals(set("avi", "bmp", "gif", "jpeg", "jpg", "mkv", "mov", "mp4", "png", "tif", "tiff", "webm"), all.get("image"));
  }

  @Test
  public void contentOfDeclaredModalitiesStartsWhateverTheErrorTable() {
    for (String errorTable : Arrays.asList("true", "false")) {
      assertEquals(Collections.emptyMap(), content(props("dummy", "m", "text", "pdf"), errorTable));
      assertEquals(Collections.emptyMap(), content(props("dummy-multimodal", "m", "image", "png,mp4"), errorTable));
      assertEquals(Collections.emptyMap(), content(props("dummy-multimodal", "m", "text,image", null), errorTable));
    }
  }

  @Test
  public void contentOfAnUndeclaredModalityIsRefusedWithTheErrorTableOff() {
    IllegalArgumentException images = assertThrows(IllegalArgumentException.class,
        () -> content(props("dummy", "m", "text", "pdf,png,jpg"), "false"));
    assertEquals("Config hoodie.unstructured.embedding.prep.file.extensions=jpg,pdf,png prepares image rows (from jpg, png), which "
        + KEY + "=text does not declare, and the error table is off (hoodie.errortable.enable): the first batch holding such a row "
        + "would fail its round on every sync. List only extensions of declared modalities in "
        + "hoodie.unstructured.embedding.prep.file.extensions, declare the modalities in " + KEY + ", or turn the error table on. "
        + "Nothing was written", images.getMessage());
    IllegalArgumentException texts = assertThrows(IllegalArgumentException.class,
        () -> content(props("dummy-multimodal", "m", "image", "pdf,png"), "false"));
    assertEquals("Config hoodie.unstructured.embedding.prep.file.extensions=pdf,png prepares text rows (from pdf), which " + KEY
        + "=image does not declare, and the error table is off (hoodie.errortable.enable): the first batch holding such a row "
        + "would fail its round on every sync. List only extensions of declared modalities in "
        + "hoodie.unstructured.embedding.prep.file.extensions, declare the modalities in " + KEY + ", or turn the error table on. "
        + "Nothing was written", texts.getMessage());
    IllegalArgumentException unset = assertThrows(IllegalArgumentException.class,
        () -> content(props("dummy", "m", "text", null), "false"));
    assertTrue(unset.getMessage().startsWith("Config hoodie.unstructured.embedding.prep.file.extensions=(unset: every "
        + "mapped extension) prepares image rows (from avi, bmp, gif,"), unset.getMessage());
  }

  @Test
  public void contentOfAnUndeclaredModalityStartsWithTheErrorTableOn() {
    assertEquals(map("image", "png"), content(props("dummy", "m", "text", "pdf,png"), "true"));
    TypedProperties speech = props("dummy-multimodal", "m", "image", "mp4");
    speech.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    assertEquals(map("text", "mp4"), content(speech, "true"), "an image-only model meets the speech text rows");
  }

  private static Map<String, Set<String>> prepared(TypedProperties props) {
    PreprocessorRegistry registry = new PreprocessorRegistry(props);
    try {
      return ModelModalityChecks.preparedModalities(EmbeddingTableConfig.from(props), registry);
    } finally {
      registry.close();
    }
  }

  private static Map<String, Set<String>> content(TypedProperties props, String errorTable) {
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), errorTable);
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    PreprocessorRegistry registry = new PreprocessorRegistry(props);
    try {
      return ModelModalityChecks.checkContent(config, registry, config.model("m1"));
    } finally {
      registry.close();
    }
  }

  private static EmbeddingModelSpec model(String provider, String name, String modalities) {
    return EmbeddingTableConfig.from(props(provider, name, modalities)).model("m1");
  }

  private static TypedProperties props(String provider, String name, String modalities) {
    return props(provider, name, modalities, null);
  }

  private static TypedProperties props(String provider, String name, String modalities, String extensions) {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);
    props.setProperty(EmbeddingTableConfig.MODELS.key(), "m1");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "8");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, provider);
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_NAME_SUFFIX, name);
    if (modalities != null) {
      props.setProperty(KEY, modalities);
    }
    if (extensions != null) {
      props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), extensions);
    }
    return props;
  }

  private static Set<String> set(String... values) {
    return new TreeSet<>(new HashSet<>(Arrays.asList(values)));
  }

  /** modality, comma-separated extensions, ... */
  private static Map<String, Set<String>> map(String... modalityAndExtensions) {
    Map<String, Set<String>> map = new TreeMap<>();
    for (int i = 0; i < modalityAndExtensions.length; i += 2) {
      map.put(modalityAndExtensions[i], set(modalityAndExtensions[i + 1].split(",")));
    }
    return map;
  }
}
