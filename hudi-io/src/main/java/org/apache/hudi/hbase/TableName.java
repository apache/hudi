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

package org.apache.hudi.hbase;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.hudi.hbase.util.Bytes;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Immutable POJO class for representing a table name.
 * Which is of the form:
 * &lt;table namespace&gt;:&lt;table qualifier&gt;
 *
 * Two special namespaces:
 *
 * 1. hbase - system namespace, used to contain hbase internal tables
 * 2. default - tables with no explicit specified namespace will
 * automatically fall into this namespace.
 *
 * ie
 *
 * a) foo:bar, means namespace=foo and qualifier=bar
 * b) bar, means namespace=default and qualifier=bar
 * c) default:bar, means namespace=default and qualifier=bar
 *
 *  <p>
 * Internally, in this class, we cache the instances to limit the number of objects and
 *  make the "equals" faster. We try to minimize the number of objects created of
 *  the number of array copy to check if we already have an instance of this TableName. The code
 *  is not optimize for a new instance creation but is optimized to check for existence.
 * </p>
 */
@InterfaceAudience.Public
public final class TableName implements Comparable<TableName> {

  /** See {@link #createTableNameIfNecessary(ByteBuffer, ByteBuffer)} */
  private static final Set<TableName> tableCache = new CopyOnWriteArraySet<>();

  /** Namespace delimiter */
  //this should always be only 1 byte long
  public final static char NAMESPACE_DELIM = ':';

  // A non-capture group so that this can be embedded.
  // regex is a bit more complicated to support nuance of tables
  // in default namespace
  //Allows only letters, digits and '_'
  public static final String VALID_NAMESPACE_REGEX =
      "(?:[_\\p{Digit}\\p{IsAlphabetic}]+)";
  //Allows only letters, digits, '_', '-' and '.'
  public static final String VALID_TABLE_QUALIFIER_REGEX =
      "(?:[_\\p{Digit}\\p{IsAlphabetic}][-_.\\p{Digit}\\p{IsAlphabetic}]*)";
  //Concatenation of NAMESPACE_REGEX and TABLE_QUALIFIER_REGEX,
  //with NAMESPACE_DELIM as delimiter
  public static final String VALID_USER_TABLE_REGEX =
      "(?:(?:(?:"+VALID_NAMESPACE_REGEX+"\\"+NAMESPACE_DELIM+")?)" +
          "(?:"+VALID_TABLE_QUALIFIER_REGEX+"))";

  /** The hbase:meta table's name. */
  public static final TableName META_TABLE_NAME =
      valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "meta");

  /** The Namespace table's name. */
  public static final TableName NAMESPACE_TABLE_NAME =
      valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "namespace");

  public static final String OLD_META_STR = ".META.";
  public static final String OLD_ROOT_STR = "-ROOT-";

  /** One globally disallowed name */
  public static final String DISALLOWED_TABLE_NAME = "zookeeper";

  /**
   * @return True if <code>tn</code> is the hbase:meta table name.
   */
  public static boolean isMetaTableName(final TableName tn) {
    return tn.equals(TableName.META_TABLE_NAME);
  }

  /**
   * TableName for old -ROOT- table. It is used to read/process old WALs which have
   * ROOT edits.
   */
  public static final TableName OLD_ROOT_TABLE_NAME = getADummyTableName(OLD_ROOT_STR);
  /**
   * TableName for old .META. table. Used in testing.
   */
  public static final TableName OLD_META_TABLE_NAME = getADummyTableName(OLD_META_STR);

  private final byte[] name;
  private final String nameAsString;
  private final byte[] namespace;
  private final String namespaceAsString;
  private final byte[] qualifier;
  private final String qualifierAsString;
  private final boolean systemTable;
  private final int hashCode;

  /**
   * Check passed byte array, "tableName", is legal user-space table name.
   * @return Returns passed <code>tableName</code> param
   * @throws IllegalArgumentException if passed a tableName is null or
   * is made of other than 'word' characters or underscores: i.e.
   * <code>[\p{IsAlphabetic}\p{Digit}.-:]</code>. The ':' is used to delimit the namespace
   * from the table name and can be used for nothing else.
   *
   * Namespace names can only contain 'word' characters
   * <code>[\p{IsAlphabetic}\p{Digit}]</code> or '_'
   *
   * Qualifier names can only contain 'word' characters
   * <code>[\p{IsAlphabetic}\p{Digit}]</code> or '_', '.' or '-'.
   * The name may not start with '.' or '-'.
   *
   * Valid fully qualified table names:
   * foo:bar, namespace=&gt;foo, table=&gt;bar
   * org:foo.bar, namespace=org, table=&gt;foo.bar
   */
  public static byte [] isLegalFullyQualifiedTableName(final byte[] tableName) {
    if (tableName == null || tableName.length <= 0) {
      throw new IllegalArgumentException("Name is null or empty");
    }

    int namespaceDelimIndex =
        org.apache.hbase.thirdparty.com.google.common.primitives.Bytes.lastIndexOf(tableName,
            (byte) NAMESPACE_DELIM);
    if (namespaceDelimIndex < 0){
      isLegalTableQualifierName(tableName);
    } else {
      isLegalNamespaceName(tableName, 0, namespaceDelimIndex);
      isLegalTableQualifierName(tableName, namespaceDelimIndex + 1, tableName.length);
    }
    return tableName;
  }

  public static byte [] isLegalTableQualifierName(final byte[] qualifierName) {
    isLegalTableQualifierName(qualifierName, 0, qualifierName.length, false);
    return qualifierName;
  }

  public static byte [] isLegalTableQualifierName(final byte[] qualifierName, boolean isSnapshot) {
    isLegalTableQualifierName(qualifierName, 0, qualifierName.length, isSnapshot);
    return qualifierName;
  }


  /**
   * Qualifier names can only contain 'word' characters
   * <code>[\p{IsAlphabetic}\p{Digit}]</code> or '_', '.' or '-'.
   * The name may not start with '.' or '-'.
   *
   * @param qualifierName byte array containing the qualifier name
   * @param start start index
   * @param end end index (exclusive)
   */
  public static void isLegalTableQualifierName(final byte[] qualifierName,
                                               int start,
                                               int end) {
    isLegalTableQualifierName(qualifierName, start, end, false);
  }

  public static void isLegalTableQualifierName(final byte[] qualifierName,
                                               int start,
                                               int end,
                                               boolean isSnapshot) {
    if(end - start < 1) {
      throw new IllegalArgumentException(isSnapshot ? "Snapshot" : "Table" + " qualifier must not be empty");
    }
    if (qualifierName[start] == '.' || qualifierName[start] == '-') {
      throw new IllegalArgumentException("Illegal first character <" + qualifierName[start] +
          "> at 0. " + (isSnapshot ? "Snapshot" : "User-space table") +
          " qualifiers can only start with 'alphanumeric " +
          "characters' from any language: " +
          Bytes.toString(qualifierName, start, end));
    }
    // Treat the bytes as UTF-8
    String qualifierString = new String(
        qualifierName, start, (end - start), StandardCharsets.UTF_8);
    if (qualifierString.equals(DISALLOWED_TABLE_NAME)) {
      // Per https://zookeeper.apache.org/doc/r3.4.10/zookeeperProgrammers.html#ch_zkDataModel
      // A znode named "zookeeper" is disallowed by zookeeper.
      throw new IllegalArgumentException("Tables may not be named '" + DISALLOWED_TABLE_NAME + "'");
    }
    for (int i = 0; i < qualifierString.length(); i++) {
      // Treat the string as a char-array as some characters may be multi-byte
      char c = qualifierString.charAt(i);
      // Check for letter, digit, underscore, hyphen, or period, and allowed by ZK.
      // ZooKeeper also has limitations, but Character.isAlphabetic omits those all
      //   See https://zookeeper.apache.org/doc/r3.4.10/zookeeperProgrammers.html#ch_zkDataModel
      if (Character.isAlphabetic(c) || Character.isDigit(c) || c == '_' || c == '-' || c == '.') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character code:" + (int) c + ", <" + c + "> at " +
          i + ". " + (isSnapshot ? "Snapshot" : "User-space table") +
          " qualifiers may only contain 'alphanumeric characters' and digits: " +
          qualifierString);
    }
  }

  public static void isLegalNamespaceName(byte[] namespaceName) {
    isLegalNamespaceName(namespaceName, 0, namespaceName.length);
  }

  /**
   * Valid namespace characters are alphabetic characters, numbers, and underscores.
   */
  public static void isLegalNamespaceName(final byte[] namespaceName,
                                          final int start,
                                          final int end) {
    if(end - start < 1) {
      throw new IllegalArgumentException("Namespace name must not be empty");
    }
    String nsString = new String(namespaceName, start, (end - start), StandardCharsets.UTF_8);
    if (nsString.equals(DISALLOWED_TABLE_NAME)) {
      // Per https://zookeeper.apache.org/doc/r3.4.10/zookeeperProgrammers.html#ch_zkDataModel
      // A znode named "zookeeper" is disallowed by zookeeper.
      throw new IllegalArgumentException("Tables may not be named '" + DISALLOWED_TABLE_NAME + "'");
    }
    for (int i = 0; i < nsString.length(); i++) {
      // Treat the string as a char-array as some characters may be multi-byte
      char c = nsString.charAt(i);
      // ZooKeeper also has limitations, but Character.isAlphabetic omits those all
      //   See https://zookeeper.apache.org/doc/r3.4.10/zookeeperProgrammers.html#ch_zkDataModel
      if (Character.isAlphabetic(c) || Character.isDigit(c)|| c == '_') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character <" + c +
          "> at " + i + ". Namespaces may only contain " +
          "'alphanumeric characters' from any language and digits: " + nsString);
    }
  }

  public byte[] getName() {
    return name;
  }

  public String getNameAsString() {
    return nameAsString;
  }

  public byte[] getNamespace() {
    return namespace;
  }

  public String getNamespaceAsString() {
    return namespaceAsString;
  }

  /**
   * Ideally, getNameAsString should contain namespace within it,
   * but if the namespace is default, it just returns the name. This method
   * takes care of this corner case.
   */
  public String getNameWithNamespaceInclAsString() {
    if(getNamespaceAsString().equals(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR)) {
      return NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR +
          TableName.NAMESPACE_DELIM + getNameAsString();
    }
    return getNameAsString();
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public String getQualifierAsString() {
    return qualifierAsString;
  }

  /**
   * @return A pointer to TableName as String bytes.
   */
  public byte[] toBytes() {
    return name;
  }

  public boolean isSystemTable() {
    return systemTable;
  }

  @Override
  public String toString() {
    return nameAsString;
  }

  /**
   *
   * @throws IllegalArgumentException See {@link #valueOf(byte[])}
   */
  private TableName(ByteBuffer namespace, ByteBuffer qualifier) throws IllegalArgumentException {
    this.qualifier = new byte[qualifier.remaining()];
    qualifier.duplicate().get(this.qualifier);
    this.qualifierAsString = Bytes.toString(this.qualifier);

    if (qualifierAsString.equals(OLD_ROOT_STR)) {
      throw new IllegalArgumentException(OLD_ROOT_STR + " has been deprecated.");
    }
    if (qualifierAsString.equals(OLD_META_STR)) {
      throw new IllegalArgumentException(OLD_META_STR + " no longer exists. The table has been " +
          "renamed to " + META_TABLE_NAME);
    }

    if (Bytes.equals(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME, namespace)) {
      // Using the same objects: this will make the comparison faster later
      this.namespace = NamespaceDescriptor.DEFAULT_NAMESPACE_NAME;
      this.namespaceAsString = NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR;
      this.systemTable = false;

      // The name does not include the namespace when it's the default one.
      this.nameAsString = qualifierAsString;
      this.name = this.qualifier;
    } else {
      if (Bytes.equals(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME, namespace)) {
        this.namespace = NamespaceDescriptor.SYSTEM_NAMESPACE_NAME;
        this.namespaceAsString = NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR;
        this.systemTable = true;
      } else {
        this.namespace = new byte[namespace.remaining()];
        namespace.duplicate().get(this.namespace);
        this.namespaceAsString = Bytes.toString(this.namespace);
        this.systemTable = false;
      }
      this.nameAsString = namespaceAsString + NAMESPACE_DELIM + qualifierAsString;
      this.name = Bytes.toBytes(nameAsString);
    }

    this.hashCode = nameAsString.hashCode();

    isLegalNamespaceName(this.namespace);
    isLegalTableQualifierName(this.qualifier);
  }

  /**
   * This is only for the old and meta tables.
   */
  private TableName(String qualifier) {
    this.qualifier = Bytes.toBytes(qualifier);
    this.qualifierAsString = qualifier;

    this.namespace = NamespaceDescriptor.SYSTEM_NAMESPACE_NAME;
    this.namespaceAsString = NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR;
    this.systemTable = true;

    // WARNING: nameAsString is different than name for old meta & root!
    // This is by design.
    this.nameAsString = namespaceAsString + NAMESPACE_DELIM + qualifierAsString;
    this.name = this.qualifier;

    this.hashCode = nameAsString.hashCode();
  }


  /**
   * Check that the object does not exist already. There are two reasons for creating the objects
   * only once:
   * 1) With 100K regions, the table names take ~20MB.
   * 2) Equals becomes much faster as it's resolved with a reference and an int comparison.
   */
  private static TableName createTableNameIfNecessary(ByteBuffer bns, ByteBuffer qns) {
    for (TableName tn : tableCache) {
      if (Bytes.equals(tn.getQualifier(), qns) && Bytes.equals(tn.getNamespace(), bns)) {
        return tn;
      }
    }

    TableName newTable = new TableName(bns, qns);
    if (tableCache.add(newTable)) {  // Adds the specified element if it is not already present
      return newTable;
    }

    // Someone else added it. Let's find it.
    for (TableName tn : tableCache) {
      if (Bytes.equals(tn.getQualifier(), qns) && Bytes.equals(tn.getNamespace(), bns)) {
        return tn;
      }
    }
    // this should never happen.
    throw new IllegalStateException(newTable + " was supposed to be in the cache");
  }


  /**
   * It is used to create table names for old META, and ROOT table.
   * These tables are not really legal tables. They are not added into the cache.
   * @return a dummy TableName instance (with no validation) for the passed qualifier
   */
  private static TableName getADummyTableName(String qualifier) {
    return new TableName(qualifier);
  }


  public static TableName valueOf(String namespaceAsString, String qualifierAsString) {
    if (namespaceAsString == null || namespaceAsString.length() < 1) {
      namespaceAsString = NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR;
    }

    for (TableName tn : tableCache) {
      if (qualifierAsString.equals(tn.getQualifierAsString()) &&
          namespaceAsString.equals(tn.getNamespaceAsString())) {
        return tn;
      }
    }

    return createTableNameIfNecessary(
        ByteBuffer.wrap(Bytes.toBytes(namespaceAsString)),
        ByteBuffer.wrap(Bytes.toBytes(qualifierAsString)));
  }


  /**
   * @throws IllegalArgumentException if fullName equals old root or old meta. Some code
   *  depends on this. The test is buried in the table creation to save on array comparison
   *  when we're creating a standard table object that will be in the cache.
   */
  public static TableName valueOf(byte[] fullName) throws IllegalArgumentException{
    for (TableName tn : tableCache) {
      if (Arrays.equals(tn.getName(), fullName)) {
        return tn;
      }
    }

    int namespaceDelimIndex =
        org.apache.hbase.thirdparty.com.google.common.primitives.Bytes.lastIndexOf(fullName,
            (byte) NAMESPACE_DELIM);

    if (namespaceDelimIndex < 0) {
      return createTableNameIfNecessary(
          ByteBuffer.wrap(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME),
          ByteBuffer.wrap(fullName));
    } else {
      return createTableNameIfNecessary(
          ByteBuffer.wrap(fullName, 0, namespaceDelimIndex),
          ByteBuffer.wrap(fullName, namespaceDelimIndex + 1,
              fullName.length - (namespaceDelimIndex + 1)));
    }
  }


  /**
   * @throws IllegalArgumentException if fullName equals old root or old meta. Some code
   *  depends on this.
   */
  public static TableName valueOf(String name) {
    for (TableName tn : tableCache) {
      if (name.equals(tn.getNameAsString())) {
        return tn;
      }
    }

    final int namespaceDelimIndex = name.indexOf(NAMESPACE_DELIM);

    if (namespaceDelimIndex < 0) {
      return createTableNameIfNecessary(
          ByteBuffer.wrap(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME),
          ByteBuffer.wrap(Bytes.toBytes(name)));
    } else {
      // indexOf is by character, not byte (consider multi-byte characters)
      String ns = name.substring(0, namespaceDelimIndex);
      String qualifier = name.substring(namespaceDelimIndex + 1);
      return createTableNameIfNecessary(
          ByteBuffer.wrap(Bytes.toBytes(ns)),
          ByteBuffer.wrap(Bytes.toBytes(qualifier)));
    }
  }


  public static TableName valueOf(byte[] namespace, byte[] qualifier) {
    if (namespace == null || namespace.length < 1) {
      namespace = NamespaceDescriptor.DEFAULT_NAMESPACE_NAME;
    }

    for (TableName tn : tableCache) {
      if (Arrays.equals(tn.getQualifier(), qualifier) &&
          Arrays.equals(tn.getNamespace(), namespace)) {
        return tn;
      }
    }

    return createTableNameIfNecessary(
        ByteBuffer.wrap(namespace), ByteBuffer.wrap(qualifier));
  }

  public static TableName valueOf(ByteBuffer namespace, ByteBuffer qualifier) {
    if (namespace == null || namespace.remaining() < 1) {
      return createTableNameIfNecessary(
          ByteBuffer.wrap(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME), qualifier);
    }

    return createTableNameIfNecessary(namespace, qualifier);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TableName tableName = (TableName) o;

    return o.hashCode() == hashCode && nameAsString.equals(tableName.nameAsString);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  /**
   * For performance reasons, the ordering is not lexicographic.
   */
  @Override
  public int compareTo(TableName tableName) {
    if (this == tableName) return 0;
    if (this.hashCode < tableName.hashCode()) {
      return -1;
    }
    if (this.hashCode > tableName.hashCode()) {
      return 1;
    }
    return this.nameAsString.compareTo(tableName.getNameAsString());
  }

}
