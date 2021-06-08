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
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.hudi.analysis.HoodieAnalysis

/**
 * The Hoodie SparkSessionExtension for extending the syntax and add the rules.
 */
class HoodieSparkSessionExtension extends (SparkSessionExtensions => Unit)
  with SparkAdapterSupport{
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // For spark2, we add a extended sql parser
    if (sparkAdapter.createExtendedSparkParser.isDefined) {
      extensions.injectParser { (session, parser) =>
        sparkAdapter.createExtendedSparkParser.get(session, parser)
      }
    }

    HoodieAnalysis.customResolutionRules().foreach { rule =>
      extensions.injectResolutionRule { session =>
        rule(session)
      }
    }

    HoodieAnalysis.customPostHocResolutionRules().foreach { rule =>
      extensions.injectPostHocResolutionRule { session =>
        rule(session)
      }
    }
  }
}
