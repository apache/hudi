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

package org.apache.hudi.index.bucket.partition;

import org.apache.hudi.exception.HoodieException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Regex-based rule engine implementation.
 */
public class RegexRuleEngine implements RuleEngine {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionBucketIndexCalculator.class);
  private static final long serialVersionUID = 1L;
  private final List<RegexRule> rules = new ArrayList<>();

  /**
   * Represents a single regex rule with its pattern and bucket number.
   */
  private static class RegexRule implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Pattern pattern;
    private final int numBuckets;

    public RegexRule(String regex, int numBuckets) {
      this.pattern = Pattern.compile(regex);
      this.numBuckets = numBuckets;
    }

    public boolean matches(String input) {
      Matcher matcher = pattern.matcher(input);
      return matcher.find();
    }

    @Override
    public String toString() {
      return pattern.pattern() + ":" + numBuckets;
    }
  }

  /**
   * Initialize the regex rule engine with expressions.
   * @param expressions Format: "expression1,bucketNumber1;expression2,bucketNumber2;...".
   */
  public RegexRuleEngine(String expressions) {
    parseExpressions(expressions);
  }

  /**
   * Parse the expressions string and create regex rules.
   */
  private void parseExpressions(String expressions) {
    String[] ruleExpressions = expressions.split(";");

    for (String ruleExpression : ruleExpressions) {
      String[] parts = ruleExpression.trim().split(",");
      if (parts.length != 2) {
        throw new HoodieException("Invalid regex expression format. Expected 'pattern,bucketNumber' but got: " + ruleExpression);
      }

      String regex = parts[0].trim();
      int bucketNumber;
      try {
        bucketNumber = Integer.parseInt(parts[1].trim());
      } catch (NumberFormatException e) {
        throw new HoodieException("Invalid bucket number in expression: " + ruleExpression, e);
      }

      rules.add(new RegexRule(regex, bucketNumber));
      LOG.info("Added regex rule: {} with bucket number: {}", regex, bucketNumber);
    }

    LOG.info("Initialized {} regex rules", rules.size());
  }

  @Override
  public int calculateNumBuckets(String partitionPath) {
    // Check each rule in order (priority by position)
    for (RegexRule rule : rules) {
      if (rule.matches(partitionPath)) {
        LOG.debug("Partition '{}' matched regex rule: {}", partitionPath, rule);
        return rule.numBuckets;
      }
    }

    // No rule matched
    return -1;
  }
}
