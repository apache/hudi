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

package org.apache.hudi.utilities.schema;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Class to provide schema for reading data and also writing into a Hoodie table,
 * used by deltastreamer (runs over Spark).
 */
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public abstract class SchemaProvider implements Serializable {

  protected TypedProperties config;

  protected JavaSparkContext jssc;

  public SchemaProvider(TypedProperties props) {
    this(props, null);
  }

  protected SchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    this.config = props;
    this.jssc = jssc;
  }

  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public abstract Schema getSourceSchema();

  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public Schema getTargetSchema() {
    // by default, use source schema as target for hoodie table as well
    return getSourceSchema();
  }
}

/**
 * The SchemaProvider class is declared as an abstract class, meaning that it cannot be instantiated directly but must be subclassed by concrete implementations.
 *    It also implements the Serializable interface, which means that instances of the class can be serialized and deserialized.
 * The class has two protected instance variables: config, which is a TypedProperties object used to hold configuration properties for the schema provider, and jssc, which is a JavaSparkContext object.
 * The constructor of the SchemaProvider class takes a TypedProperties object as its argument and initializes the config variable with it.
 *    There is also a protected constructor that takes both a TypedProperties object and a JavaSparkContext object, which initializes both config and jssc instance variables.
 *      A protected constructor is a type of constructor in object-oriented programming languages that can only be accessed by subclasses or classes within the same package as the class that defines it.
 *      In other words, only the class itself, its subclasses, and other classes in the same package can create an instance of the class using a protected constructor.
 * The SchemaProvider class provides two abstract methods that must be implemented by concrete subclasses:
 *    getSourceSchema(), which returns the schema of the data source
 *    getTargetSchema(), which returns the schema of the target Hoodie table.
 */