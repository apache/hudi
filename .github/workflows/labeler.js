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

async function labelDocsPr({ github, context, prNumber }) {
  await github.rest.issues.addLabels({
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: prNumber,
    labels: ['docs']
  });

  console.log(`- Labeled Docs PR: ${prNumber}`);
}

async function labelPrWithSize({ github, context, prNumber, prData }) {
  console.log(`Label PR based on size: ${prNumber} ${prData.html_url}`);
  const additions = prData.additions;
  const deletions = prData.deletions;
  const totalChanges = additions + deletions;

  let label = "";

  if (totalChanges <= 10) {
    label = "size:XS";
  } else if (totalChanges <= 100) {
    label = "size:S";
  } else if (totalChanges <= 300) {
    label = "size:M";
  } else if (totalChanges <= 1000) {
    label = "size:L";
  } else {
    label = "size:XL";
  }

  // Apply the label
  await github.rest.issues.addLabels({
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: prNumber,
    labels: [label]
  });

  console.log(`Total lines of changes: ${totalChanges}`);
  console.log(`Label: ${label}`);
}

module.exports = {
  labelDocsPr,
  labelPrWithSize
};
