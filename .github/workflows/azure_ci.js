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

async function checkAzureCiAndCreateCommitStatus({ github, context, prNumber, latestCommitHash }) {
  console.log(`- Checking Azure CI status of PR: ${prNumber} ${latestCommitHash}`);
  const botUsername = 'hudi-bot';

  const comments = await github.paginate(github.rest.issues.listComments, {
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: prNumber,
    sort: 'updated',
    direction: 'desc',
    per_page: 100
  });

  // Find the latest comment from hudi-bot containing the Azure CI report
  const botComments = comments.filter(comment => comment.user.login === botUsername);

  let status = 'pending';
  let message = 'In progress';
  let azureRunLink = '';

  if (botComments.length > 0) {
    const lastComment = botComments[0];
    const reportPrefix = `${latestCommitHash} Azure: `
    const successReportString = `${reportPrefix}[SUCCESS]`
    const failureReportString = `${reportPrefix}[FAILURE]`

    if (lastComment.body.includes(reportPrefix)) {
      if (lastComment.body.includes(successReportString)) {
        message = 'Successful on the latest commit';
        status = 'success';
      } else if (lastComment.body.includes(failureReportString)) {
        message = 'Failed on the latest commit';
        status = 'failure';
      }
    }

    const linkRegex = /\[[a-zA-Z]+\]\((https?:\/\/[^\s]+)\)/;
    const parts = lastComment.body.split(reportPrefix);
    const secondPart = parts.length > 1 ? parts[1] : '';
    const match = secondPart.match(linkRegex);

    if (match) {
      azureRunLink = match[1];
    }
  }

  console.log(`Status: ${status}`);
  console.log(`Azure Run Link: ${azureRunLink}`);
  console.log(`${message}`);

  console.log(`- Create commit status of PR based on Azure CI status: ${prNumber} ${latestCommitHash}`);
  // Create or update the commit status for Azure CI
  await github.rest.repos.createCommitStatus({
    owner: context.repo.owner,
    repo: context.repo.repo,
    sha: latestCommitHash,
    state: status,
    target_url: azureRunLink,
    description: message,
    context: 'Azure CI'
  });

  return { status, message, azureRunLink };
}

module.exports = checkAzureCiAndCreateCommitStatus;
