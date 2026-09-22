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

package org.apache.spark.sql.hudi

import org.apache.hudi.SparkAdapterSupport

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hudi.analysis.HoodieAnalysis
import org.apache.spark.sql.parser.HoodieCommonSqlParser

/**
 * The Hoodie SparkSessionExtension for extending the syntax and add the rules.
 */
class HoodieSparkSessionExtension extends (SparkSessionExtensions => Unit)
  with SparkAdapterSupport {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (session, parser) =>
      new HoodieCommonSqlParser(session, parser)
    }

    HoodieAnalysis.customResolutionRules.foreach { ruleBuilder =>
      extensions.injectResolutionRule(ruleBuilder(_))
    }

    HoodieAnalysis.customPostHocResolutionRules.foreach { ruleBuilder =>
      extensions.injectPostHocResolutionRule(ruleBuilder(_))
    }

    HoodieAnalysis.customOptimizerRules.foreach { ruleBuilder =>
      extensions.injectOptimizerRule(ruleBuilder(_))
    }

    extensions.injectOptimizerRule(HoodieNestedPredicatePushdownConfig(_))

    /*
    // CBO is only supported in Spark >= 3.1.x
    HoodieAnalysis.customPreCBORules.foreach { ruleBuilder =>
      extensions.injectPreCBORule(ruleBuilder(_))
    }
    */

    sparkAdapter.injectTableFunctions(extensions)
    sparkAdapter.injectScalarFunctions(extensions)
    sparkAdapter.injectPlannerStrategies(extensions)
  }
}

/**
 * Ensures that Spark's nested predicate pushdown allowlist includes "hoodiefilegroup"
 * so that nested struct field predicates (e.g., msg.field = 'value') are pushed down
 * to the Parquet reader when using HoodieFileGroupReaderBasedFileFormat.
 */
case class HoodieNestedPredicatePushdownConfig(spark: SparkSession) extends Rule[LogicalPlan] with Logging {
  @volatile private var applied = false

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!applied) {
      val key = "spark.sql.optimizer.nestedPredicatePushdown.supportedFileSources"
      val current = spark.conf.get(key, "parquet,orc")
      if (!current.toLowerCase.contains("hoodiefilegroup")) {
        val updated = current + ",hoodiefilegroup"
        spark.conf.set(key, updated)
        logInfo(s"Added hoodiefilegroup to $key: $updated")
      }
      applied = true
    }
    plan
  }
}
