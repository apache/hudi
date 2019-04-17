/*
 *  Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.util;

import com.uber.hoodie.exception.HoodieSerializationException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * (NOTE: Adapted from Apache commons-lang3)
 * This class defines API's to serde an object.
 */
public class SerializationUtils {
  // Serialize
  //-----------------------------------------------------------------------

  /**
   * <p>Serializes an {@code Object} to the specified stream.</p>
   *
   * <p>The stream will be closed once the object is written.
   * This avoids the need for a finally clause, and maybe also exception
   * handling, in the application code.</p>
   *
   * <p>The stream passed in is not buffered internally within this method.
   * This is the responsibility of your application if desired.</p>
   *
   * @param obj the object to serialize to bytes, may be null
   * @param outputStream the stream to write to, must not be null
   * @throws IllegalArgumentException if {@code outputStream} is {@code null}
   * @throws HoodieSerializationException (runtime) if the serialization fails
   */
  public static void serialize(final Serializable obj, final OutputStream outputStream) {
    if (outputStream == null) {
      throw new IllegalArgumentException("The OutputStream must not be null");
    }
    ObjectOutputStream out = null;
    try {
      // stream closed in the finally
      out = new ObjectOutputStream(outputStream);
      out.writeObject(obj);

    } catch (final IOException ex) {
      throw new HoodieSerializationException("unable to serialize object", ex);
    } finally {
      try {
        if (out != null) {
          out.close();
        }
      } catch (final IOException ex) { // NOPMD
        // ignore close exception
      }
    }
  }

  /**
   * <p>Serializes an {@code Object} to a byte array for
   * storage/serialization.</p>
   *
   * @param obj the object to serialize to bytes
   * @return a byte[] with the converted Serializable
   * @throws HoodieSerializationException (runtime) if the serialization fails
   */
  public static byte[] serialize(final Serializable obj) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
    serialize(obj, baos);
    return baos.toByteArray();
  }

  // Deserialize
  //-----------------------------------------------------------------------

  /**
   * <p>
   * Deserializes an {@code Object} from the specified stream.
   * </p>
   *
   * <p>
   * The stream will be closed once the object is written. This avoids the need for a finally clause, and maybe also
   * exception handling, in the application code.
   * </p>
   *
   * <p>
   * The stream passed in is not buffered internally within this method. This is the responsibility of your
   * application if desired.
   * </p>
   *
   * <p>
   * If the call site incorrectly types the return value, a {@link ClassCastException} is thrown from the call site.
   * Without Generics in this declaration, the call site must type cast and can cause the same ClassCastException.
   * Note that in both cases, the ClassCastException is in the call site, not in this method.
   * </p>
   *
   * @param <T> the object type to be deserialized
   * @param inputStream the serialized object input stream, must not be null
   * @return the deserialized object
   * @throws IllegalArgumentException if {@code inputStream} is {@code null}
   * @throws HoodieSerializationException (runtime) if the serialization fails
   */
  public static <T> T deserialize(final InputStream inputStream) {
    if (inputStream == null) {
      throw new IllegalArgumentException("The InputStream must not be null");
    }
    ObjectInputStream in = null;
    try {
      // stream closed in the finally
      in = new ObjectInputStream(inputStream);
      @SuppressWarnings("unchecked") // may fail with CCE if serialised form is incorrect
      final T obj = (T) in.readObject();
      return obj;

    } catch (final ClassCastException ex) {
      throw new HoodieSerializationException("cannot cast class", ex);
    } catch (final ClassNotFoundException ex) {
      throw new HoodieSerializationException("class not found", ex);
    } catch (final IOException ex) {
      throw new HoodieSerializationException("unable to deserialize to object", ex);
    } finally {
      try {
        if (in != null) {
          in.close();
        }
      } catch (final IOException ex) { // NOPMD
        // ignore close exception
      }
    }
  }

  /**
   * <p>
   * Deserializes a single {@code Object} from an array of bytes.
   * </p>
   *
   * <p>
   * If the call site incorrectly types the return value, a {@link ClassCastException} is thrown from the call site.
   * Without Generics in this declaration, the call site must type cast and can cause the same ClassCastException.
   * Note that in both cases, the ClassCastException is in the call site, not in this method.
   * </p>
   *
   * @param <T> the object type to be deserialized
   * @param objectData the serialized object, must not be null
   * @return the deserialized object
   * @throws IllegalArgumentException if {@code objectData} is {@code null}
   * @throws HoodieSerializationException (runtime) if the serialization fails
   */
  public static <T> T deserialize(final byte[] objectData) {
    if (objectData == null) {
      throw new IllegalArgumentException("The byte[] must not be null");
    }
    return deserialize(new ByteArrayInputStream(objectData));
  }
}
