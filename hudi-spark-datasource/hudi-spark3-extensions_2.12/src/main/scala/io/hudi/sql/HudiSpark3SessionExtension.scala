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

package io.hudi.sql

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.analysis.{HudiAnalysis, HudiOperationsCheck, PostprocessHudiTable, ProcessHudiMerge}
import org.apache.spark.sql.catalyst.optimizer.{OptimizerconditionHudi, RewriteHudiUID}

/**
  * An extension for Spark SQL to activate Hudi SQL parser to support Hudi SQL grammer
  * example to create a 'SparkSession' with the hudi SQL grammar
  * {{{
  *   import org.apache.spark.sql.SparkSession
  *
  *   val spark = SparkSession
  *   .builder()
  *   .appName("...")
  *   .master("...")
  *   .config("spark.sql.extensions", "io.hudi.sql.HudiSpark3SessionExtension")
  * }}}
  *
  */
class HudiSpark3SessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    //
    extensions.injectResolutionRule { session =>
      new HudiAnalysis(session, session.sessionState.conf)
    }
    //
    extensions.injectCheckRule { session =>
      new HudiOperationsCheck(session)
    }
    //
    extensions.injectPostHocResolutionRule { session =>
      new ProcessHudiMerge(session, session.sessionState.conf)
    }
    //
    extensions.injectPostHocResolutionRule { session =>
      new PostprocessHudiTable(session)
    }
    extensions.injectOptimizerRule { _ =>
      OptimizerconditionHudi
    }
    extensions.injectOptimizerRule { _ =>
      RewriteHudiUID
    }
  }
}
