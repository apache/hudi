/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.conf;

import java.util.Map;

/**
 * Configuration interface with the methods necessary to configure Parquet applications.
 */
public interface ParquetConfiguration extends Iterable<Map.Entry<String, String>> {

  /**
   * Sets the value of the name property.
   *
   * @param name  the property to set
   * @param value the value to set the property to
   */
  void set(String name, String value);

  /**
   * Sets the value of the name property to a long.
   *
   * @param name  the property to set
   * @param value the value to set the property to
   */
  void setLong(String name, long value);

  /**
   * Sets the value of the name property to an integer.
   *
   * @param name  the property to set
   * @param value the value to set the property to
   */
  void setInt(String name, int value);

  /**
   * Sets the value of the name property to a boolean.
   *
   * @param name  the property to set
   * @param value the value to set the property to
   */
  void setBoolean(String name, boolean value);

  /**
   * Sets the value of the name property to an array of comma delimited values.
   *
   * @param name  the property to set
   * @param value the values to set the property to
   */
  void setStrings(String name, String... value);

  /**
   * Sets the value of the name property to a class.
   *
   * @param name  the property to set
   * @param value the value to set the property to
   * @param xface the interface implemented by the value
   */
  void setClass(String name, Class<?> value, Class<?> xface);

  /**
   * Gets the value of the name property. Returns null if no such value exists.
   *
   * @param name the property to retrieve the value of
   * @return the value of the property, or null if it does not exist
   */
  String get(String name);

  /**
   * Gets the value of the name property. Returns the default value if no such value exists.
   *
   * @param name         the property to retrieve the value of
   * @param defaultValue the default return if no value is set for the property
   * @return the value of the property, or the default value if it does not exist
   */
  String get(String name, String defaultValue);

  /**
   * Gets the value of the name property as a long. Returns the default value if no such value exists.
   *
   * @param name         the property to retrieve the value of
   * @param defaultValue the default return if no value is set for the property
   * @return the value of the property as a long, or the default value if it does not exist
   */
  long getLong(String name, long defaultValue);

  /**
   * Gets the value of the name property as an integer. Returns the default value if no such value exists.
   *
   * @param name         the property to retrieve the value of
   * @param defaultValue the default return if no value is set for the property
   * @return the value of the property as an integer, or the default value if it does not exist
   */
  int getInt(String name, int defaultValue);

  /**
   * Gets the value of the name property as a boolean. Returns the default value if no such value exists.
   *
   * @param name         the property to retrieve the value of
   * @param defaultValue the default return if no value is set for the property
   * @return the value of the property as a boolean, or the default value if it does not exist
   */
  boolean getBoolean(String name, boolean defaultValue);

  /**
   * Gets the trimmed value of the name property. Returns null if no such value exists.
   *
   * @param name the property to retrieve the value of
   * @return the trimmed value of the property, or null if it does not exist
   */
  String getTrimmed(String name);

  /**
   * Gets the trimmed value of the name property as a boolean.
   * Returns the default value if no such value exists.
   *
   * @param name         the property to retrieve the value of
   * @param defaultValue the default return if no value is set for the property
   * @return the trimmed value of the property, or the default value if it does not exist
   */
  String getTrimmed(String name, String defaultValue);

  /**
   * Gets the value of the name property as an array of {@link String}s.
   * Returns the default value if no such value exists.
   * Interprets the stored value as a comma delimited array.
   *
   * @param name         the property to retrieve the value of
   * @param defaultValue the default return if no value is set for the property
   * @return the value of the property as an array, or the default value if it does not exist
   */
  String[] getStrings(String name, String[] defaultValue);

  /**
   * Gets the value of the name property as a class. Returns the default value if no such value exists.
   *
   * @param name         the property to retrieve the value of
   * @param defaultValue the default return if no value is set for the property
   * @return the value of the property as a class, or the default value if it does not exist
   */
  Class<?> getClass(String name, Class<?> defaultValue);

  /**
   * Gets the value of the name property as a class implementing the xface interface.
   * Returns the default value if no such value exists.
   *
   * @param name         the property to retrieve the value of
   * @param defaultValue the default return if no value is set for the property
   * @return the value of the property as a class, or the default value if it does not exist
   */
  <U> Class<? extends U> getClass(String name, Class<? extends U> defaultValue, Class<U> xface);

  /**
   * Load a class by name.
   *
   * @param name the name of the {@link Class} to load
   * @return the loaded class
   * @throws ClassNotFoundException when the specified class cannot be found
   */
  Class<?> getClassByName(String name) throws ClassNotFoundException;
}
