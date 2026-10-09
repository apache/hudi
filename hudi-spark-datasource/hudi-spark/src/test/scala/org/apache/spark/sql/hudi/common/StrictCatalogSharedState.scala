/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.spark.sql.hudi.common

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, SparkSessionExtensionsProvider}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogUtils, ExternalCatalogEvent, ExternalCatalogWithListener}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog.DEFAULT_DATABASE
import org.apache.spark.sql.internal.{SharedState, StaticSQLConf}
import org.apache.spark.util.Utils

import java.util.ServiceLoader

import scala.collection.JavaConverters._

/**
 * [[SharedState]] whose external catalog is a [[StrictDefaultDatabaseCatalog]], so every
 * session built on it rejects table and function operations on the `default` database.
 */
class StrictCatalogSharedState(sc: SparkContext, initialConfigs: Map[String, String])
  extends SharedState(sc, initialConfigs) {

  override lazy val externalCatalog: ExternalCatalogWithListener = {
    val catalog = new StrictDefaultDatabaseCatalog(conf, hadoopConf)
    if (!catalog.databaseExists(DEFAULT_DATABASE)) {
      catalog.createDatabase(CatalogDatabase(DEFAULT_DATABASE, "default database",
        CatalogUtils.stringToURI(conf.get(StaticSQLConf.WAREHOUSE_PATH)), Map()), ignoreIfExists = true)
    }
    val wrapped = new ExternalCatalogWithListener(catalog)
    wrapped.addListener((event: ExternalCatalogEvent) => sparkContext.listenerBus.post(event))
    wrapped
  }
}

object StrictCatalogSharedState {

  /**
   * Creates a session on a [[StrictCatalogSharedState]] the way `SparkSession.Builder.getOrCreate`
   * would (same context, extensions and initial options), and makes it the default and active session.
   */
  def createSession(options: Map[String, String]): SparkSession = {
    val sparkConf = new SparkConf()
    options.foreach { case (k, v) => sparkConf.set(k, v) }
    val sparkContext = SparkContext.getOrCreate(sparkConf)
    val session = newSession(sparkContext, new StrictCatalogSharedState(sparkContext, options),
      applyExtensions(sparkContext, new SparkSessionExtensions), options)
    SparkSession.setDefaultSession(session)
    SparkSession.setActiveSession(session)
    session
  }

  private def applyExtensions(sparkContext: SparkContext, extensions: SparkSessionExtensions): SparkSessionExtensions = {
    ServiceLoader.load(classOf[SparkSessionExtensionsProvider], Utils.getContextOrSparkClassLoader)
      .asScala.foreach(_.apply(extensions))
    sparkContext.getConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS).getOrElse(Seq.empty).foreach { className =>
      Utils.classForName[AnyRef](className).getConstructor().newInstance()
        .asInstanceOf[SparkSessionExtensions => Unit].apply(extensions)
    }
    extensions
  }

  /**
   * The only `SparkSession` constructor that accepts an existing [[SharedState]] is class-private
   * (and lives on `org.apache.spark.sql.classic.SparkSession` with an extra options map in Spark 4),
   * so it is invoked reflectively; any trailing map parameters are passed empty.
   */
  private def newSession(sparkContext: SparkContext, sharedState: SharedState,
                         extensions: SparkSessionExtensions, options: Map[String, String]): SparkSession = {
    val sessionClass: Class[_] = try {
      Utils.classForName[AnyRef]("org.apache.spark.sql.classic.SparkSession")
    } catch {
      case _: ClassNotFoundException => classOf[SparkSession]
    }
    val constructor = sessionClass.getConstructors.find { c =>
      val params = c.getParameterTypes
      params.length >= 5 && params(0) == classOf[SparkContext] && params(1) == classOf[Option[_]]
    }.getOrElse(throw new IllegalStateException(s"No SparkSession constructor taking a SharedState on $sessionClass"))
    val args: Seq[AnyRef] = Seq(sparkContext, Some(sharedState), None, extensions, options) ++
      Seq.fill(constructor.getParameterCount - 5)(Map.empty[String, String])
    constructor.newInstance(args: _*).asInstanceOf[SparkSession]
  }
}
