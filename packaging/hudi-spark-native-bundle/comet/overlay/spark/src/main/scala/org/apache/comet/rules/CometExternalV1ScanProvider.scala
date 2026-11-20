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

package org.apache.comet.rules

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.FileFormat

/**
 * Converts a data source V1 scan over a file format Comet does not know into a Comet native scan.
 * Implementations ship out of tree and register through the JDK `ServiceLoader` with a
 * `META-INF/services/org.apache.comet.rules.CometExternalV1ScanProvider` resource, the same
 * mechanism `PlanDataInjector` uses for per-partition planning data.
 */
trait CometExternalV1ScanProvider {

  /** Whether this provider recognizes the scan's file format. */
  def supportsFormat(fileFormat: FileFormat): Boolean

  /**
   * Converts the scan to a Comet plan, or None to leave the scan to Spark. A provider returns
   * None for shapes it does not handle (query types, schema features); the scan then falls back
   * exactly as an unsupported format does.
   */
  def convert(session: SparkSession, scanExec: FileSourceScanExec): Option[SparkPlan]
}

object CometExternalV1ScanProvider extends Logging {

  private[comet] lazy val providers: Seq[CometExternalV1ScanProvider] = {
    try {
      ServiceLoader
        .load(classOf[CometExternalV1ScanProvider], getClass.getClassLoader)
        .asScala
        .toSeq
    } catch {
      case NonFatal(e) =>
        logWarning("Failed to load CometExternalV1ScanProvider services", e)
        Seq.empty
    }
  }

  def findForFormat(fileFormat: FileFormat): Option[CometExternalV1ScanProvider] =
    providers.find(_.supportsFormat(fileFormat))
}
