/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

async function check() {

    try {
        const core = require('@actions/core');
        const {context, GitHub} = require('@actions/github');

        const github = new GitHub(process.env.GITHUB_TOKEN);
        const provider = core.getInput('provider', {required: true});
        const repository = core.getInput('repository', {required: true});
        const command = core.getInput('command', {required: true});

        if (command !== 'rerun tests') {
            console.log("Invalid command:" + command);
            return;
        }

        const {
            data: {
                head: {
                    sha: ref,
                }
            }
        } = await github.pulls.get({
            owner: provider,
            repo: repository,
            pull_number: context.issue.number,
        });

        const checks = await github.checks.listForRef({
            owner: provider,
            repo: repository,
            ref: ref
        });

        checks.data.check_runs.forEach(run => {

            if (run.app.owner.login === 'travis-ci') {
                console.log("rerun travis ci check")
                rebuild(run.external_id)
            } else {
                console.log("ignore github action check")
            }

        });

    } catch (e) {
        console.log(e)
    }

}

function rebuild(buildId) {
    const https = require('https');
    const token = process.env.HUDI_TRAVIS_ORG_TOKEN

    const options = {
        hostname: 'api.travis-ci.org',
        port: 443,
        path: `/build/${buildId}/restart`,
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Travis-API-Version': 3,
            'Authorization': `token ${token}`,
        }
    };

    const req = https.request(options, function (res) {
        res.setEncoding('utf8');
        res.on('data', function (data) {
            console.log('data: ' + data);
        });
        res.on('error', function (error) {
            console.log('error: ' + error);
        });
    });
    req.on('error', function (e) {
        console.log('problem with request: ' + e.message);
    });

    req.end();
}

module.exports = check;
