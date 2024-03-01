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

  let newSizeLabel = "";

  if (totalChanges <= 10) {
    // size:XS : <= 10 LoC
    newSizeLabel = "size:XS";
  } else if (totalChanges <= 100) {
    // size:S : (10, 100] LoC
    newSizeLabel = "size:S";
  } else if (totalChanges <= 300) {
    // size:M : (100, 300] LoC
    newSizeLabel = "size:M";
  } else if (totalChanges <= 1000) {
    // size:L : (300, 1000] LoC
    newSizeLabel = "size:L";
  } else {
    // size:XL : > 1000 LoC
    newSizeLabel = "size:XL";
  }

  // Check existing size label
  const { data: labels } = await github.rest.issues.listLabelsOnIssue({
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: prNumber
  });

  const existingSizeLabels = labels.filter(label => label.name.startsWith("size:") && label.name !== newSizeLabel);
  const newSizeLabelInExisting = labels.filter(label => label.name === newSizeLabel);

  // Remove stale labels that do not match the new one
  for (const label of existingSizeLabels) {
    await github.rest.issues.removeLabel({
      owner: context.repo.owner,
      repo: context.repo.repo,
      issue_number: prNumber,
      name: label.name,
    });
    console.log(`Removed stale size label: ${label.name}`);
  }

  console.log(`Total lines of changes: ${totalChanges}`);

  // Add the new size label if needed
  if (newSizeLabelInExisting.length > 0) {
    console.log(`Accurate size Label already exists: ${newSizeLabel}`);
  } else {
    // Add the new label
    await github.rest.issues.addLabels({
      owner: context.repo.owner,
      repo: context.repo.repo,
      issue_number: prNumber,
      labels: [newSizeLabel]
    });
    console.log(`Added size Label: ${newSizeLabel}`);
  }
}

module.exports = {
  labelDocsPr,
  labelPrWithSize
};
