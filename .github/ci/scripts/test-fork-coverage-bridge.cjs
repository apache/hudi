// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Tests the github-script steps of fork_coverage_bridge.yml against mocked GitHub APIs, in the
// style of test-fork-ci-status.cjs.
const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const { readFileSync } = require('node:fs');
const path = require('node:path');
const { test } = require('node:test');

const bridgeWorkflow = JSON.parse(execFileSync('python3', [
  '-c', 'import json, sys, yaml; print(json.dumps(yaml.safe_load(sys.stdin)))',
], {
  input: readFileSync(path.join(__dirname, '../../workflows/fork_coverage_bridge.yml')),
  encoding: 'utf8',
}));
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const select = new AsyncFunction('github', 'context', 'core', 'Date',
  bridgeWorkflow.jobs.select.steps[0].with.script);
const record = new AsyncFunction('github', 'context', 'core', 'process',
  bridgeWorkflow.jobs.record.steps[0].with.script);
const now = Date.UTC(2026, 9, 20, 12);
class Clock extends Date { static now() { return now; } }
const MB = 1024 * 1024;

function makePr(number, { sha = String(number % 10).repeat(40), owner = 'contributor', ref = 'feature',
  base = 'master', updated = now, full_name } = {}) {
  return {
    number, state: 'open', updated_at: new Date(updated).toISOString(),
    head: { sha, ref, repo: { owner: { login: owner }, name: 'hudi', full_name: full_name || owner + '/hudi' } },
    base: { ref: base },
  };
}
const pr = makePr(10);
const sha = pr.head.sha;
const run = (id = 1, extra = {}) => ({
  id, run_attempt: 1, event: 'push', head_sha: sha, head_branch: 'feature', status: 'completed', conclusion: 'success',
  ...extra,
});
let nextArtifactId = 1000;
const artifact = (name, extra = {}) => ({ id: nextArtifactId++, name, expired: false, size_in_bytes: MB, ...extra });
const coverage = (job, flag, headSha = sha) => 'coverage-' + headSha + '__' + job + '__' + flag;
// The runs of a fork, keyed by workflow file. By default bot.yml has run 1 and java_ci_engines.yml run 2.
const workflows = (bot = [run(1)], engines = [run(2)]) => ({ 'bot.yml': bot, 'java_ci_engines.yml': engines });
const WORKFLOW_IDS = ['bot.yml', 'java_ci_engines.yml'];
const runIds = (bot, engines, botAttempt = 1, enginesAttempt = 1) => [
  { workflow: 'bot.yml', run_id: bot, run_attempt: botAttempt },
  { workflow: 'java_ci_engines.yml', run_id: engines, run_attempt: enginesAttempt },
];

// runs are keyed by fork owner and workflow file, artifacts by run id; a missing key answers 404.
async function executeSelect({ pulls = [pr], runs = { contributor: workflows() }, artifacts, checks = [] } = {}) {
  artifacts = artifacts || {
    1: [artifact(coverage('test-utilities', 'utilities'))],
    2: [artifact(coverage('test-flink-1', 'flink-integration-tests'))],
  };
  const outputs = {}, warnings = [], calls = [];
  const github = { rest: { checks: {}, actions: {}, pulls: {} } };
  const notFound = () => Object.assign(new Error('Not Found'), { status: 404 });
  github.rest.pulls.list = async () => {};
  github.rest.checks.listForRef = async () => {};
  github.rest.actions.listWorkflowRunArtifacts = async () => {};
  github.rest.actions.listWorkflowRuns = async (params) => {
    calls.push(['runs', params]);
    assert.ok(WORKFLOW_IDS.includes(params.workflow_id), params.workflow_id);
    if (!(params.owner in runs)) throw notFound();
    const fork = runs[params.owner];
    if (!(params.workflow_id in fork)) throw notFound();
    return { data: { workflow_runs: fork[params.workflow_id] } };
  };
  github.paginate = async (fn, params) => {
    if (fn === github.rest.checks.listForRef) {
      assert.equal(params.owner, 'apache');
      assert.equal(params.check_name, 'Coverage (fork)');
      return checks.filter((item) => item.ref === params.ref);
    }
    assert.equal(fn, github.rest.actions.listWorkflowRunArtifacts);
    calls.push(['artifacts', params]);
    if (!(params.run_id in artifacts)) throw notFound();
    return artifacts[params.run_id];
  };
  github.paginate.iterator = async function* (fn, params) {
    assert.equal(fn, github.rest.pulls.list);
    assert.equal(params.base, 'master');
    assert.equal(params.sort, 'updated');
    for (let i = 0; i < pulls.length; i += 100) yield { data: pulls.slice(i, i + 100) };
  };
  await select(github, { repo: { owner: 'apache', repo: 'hudi' } }, {
    info: () => {}, warning: (message) => warnings.push(message),
    setOutput: (name, value) => { outputs[name] = JSON.parse(value); },
  }, Clock);
  return { ...outputs, warnings, calls };
}

test('selects the artifacts of both completed fork runs under their own run ids', async () => {
  const { artifacts, prs, calls } = await executeSelect();
  const entry = { pr: 10, sha, branch: 'contributor:feature', fork: 'contributor/hudi' };
  assert.deepEqual(artifacts, [
    { ...entry, workflow: 'bot.yml', run_id: 1, artifact_id: artifacts[0].artifact_id, flags: 'utilities' },
    { ...entry, workflow: 'java_ci_engines.yml', run_id: 2, artifact_id: artifacts[1].artifact_id,
      flags: 'flink-integration-tests' },
  ]);
  assert.deepEqual(prs, [{ pr: 10, sha, runs: runIds(1, 2) }]);
  assert.deepEqual(calls.map(([kind, params]) => [kind, params.workflow_id || params.run_id]), [
    ['runs', 'bot.yml'], ['runs', 'java_ci_engines.yml'], ['artifacts', 1], ['artifacts', 2],
  ]);
});
test('same-repository PRs and PRs to other branches are ignored', async () => {
  const result = await executeSelect({ pulls: [
    makePr(10, { owner: 'apache', full_name: 'Apache/Hudi' }),
    makePr(10, { base: 'release-1.2' }),
  ] });
  assert.deepEqual(result.artifacts, []);
  assert.deepEqual(result.prs, []);
  assert.deepEqual(result.calls, []);
});
test('only completed push and manual runs of the current head are used', async () => {
  const { artifacts, prs } = await executeSelect({
    runs: { contributor: workflows([
      run(9, { status: 'in_progress', conclusion: null }),
      run(8, { event: 'pull_request' }),
      run(7, { head_branch: 'other' }),
      run(6, { event: 'workflow_dispatch' }),
      run(5),
    ]) },
    artifacts: { 6: [artifact(coverage('test-utilities', 'utilities'))] },
  });
  assert.deepEqual(artifacts.map((item) => item.run_id), [6]);
  assert.deepEqual(prs, [{ pr: 10, sha, runs: runIds(6, 2) }]);
});
test('a PR whose head moved past either fork run is skipped', async () => {
  const moved = { head_sha: 'f'.repeat(40) };
  for (const runs of [workflows([run(1, moved)]), workflows(undefined, [run(2, moved)])]) {
    const result = await executeSelect({ runs: { contributor: runs } });
    assert.deepEqual(result.artifacts, []);
    assert.deepEqual(result.prs, []);
    assert.equal(result.calls.filter(([kind]) => kind === 'artifacts').length, 0);
  }
});
test('a PR is skipped while either workflow has no completed run', async () => {
  const running = { status: 'in_progress', conclusion: null };
  for (const runs of [workflows(undefined, [run(2, running)]), workflows([run(1, running)])]) {
    const result = await executeSelect({ runs: { contributor: runs } });
    assert.deepEqual(result.artifacts, []);
    assert.deepEqual(result.prs, []);
    assert.equal(result.calls.filter(([kind]) => kind === 'artifacts').length, 0);
    assert.deepEqual(result.warnings, []);
  }
});
test('a fork without one of the workflows is skipped without a warning', async () => {
  for (const runs of [{ 'bot.yml': [run(1)] }, { 'java_ci_engines.yml': [run(2)] }]) {
    const result = await executeSelect({ runs: { contributor: runs } });
    assert.deepEqual(result.artifacts, []);
    assert.deepEqual(result.prs, []);
    assert.equal(result.calls.filter(([kind]) => kind === 'artifacts').length, 0);
    assert.deepEqual(result.warnings, []);
  }
});
test('a PR recorded with both current run ids is skipped, a foreign check is not', async () => {
  const recorded = { ref: sha, app: { slug: 'github-actions' }, external_id: 'hudi-fork-coverage:10:1.1:2.1' };
  const skipped = await executeSelect({ checks: [recorded] });
  assert.deepEqual(skipped.artifacts, []);
  assert.deepEqual(skipped.prs, []);
  assert.equal(skipped.calls.filter(([kind]) => kind === 'artifacts').length, 0);
  for (const check of [
    { ...recorded, app: { slug: 'other-app' } },
    { ...recorded, external_id: 'hudi-fork-coverage:20:1.1:2.1' },
    { ...recorded, external_id: 'hudi-fork-coverage:10:1:2' },
  ]) {
    assert.equal((await executeSelect({ checks: [check] })).artifacts.length, 2);
  }
});
test('a new run of either workflow changes the id, so the PR is uploaded again', async () => {
  const recorded = { ref: sha, app: { slug: 'github-actions' }, external_id: 'hudi-fork-coverage:10:1.1:2.1' };
  const artifacts = {
    1: [artifact(coverage('test-utilities', 'utilities'))],
    2: [artifact(coverage('test-flink-1', 'flink-integration-tests'))],
    3: [artifact(coverage('test-utilities', 'utilities'))],
    4: [artifact(coverage('test-flink-1', 'flink-integration-tests'))],
  };
  for (const [runs, ids] of [
    [workflows([run(3, { event: 'workflow_dispatch' }), run(1)]), [3, 2]],
    [workflows(undefined, [run(4, { event: 'workflow_dispatch' }), run(2)]), [1, 4]],
  ]) {
    const result = await executeSelect({ runs: { contributor: runs }, artifacts, checks: [recorded] });
    assert.deepEqual(result.prs, [{ pr: 10, sha, runs: runIds(...ids) }]);
    assert.deepEqual(new Set(result.artifacts.map((item) => item.run_id)), new Set(ids));
  }
});
test('a re-run of either workflow keeps the run id but is uploaded again', async () => {
  const recorded = { ref: sha, app: { slug: 'github-actions' }, external_id: 'hudi-fork-coverage:10:1.2:2.1' };
  const name = coverage('test-utilities', 'utilities');
  // The re-run job overwrote its artifact, which got a new id.
  const rerun = artifact(name);
  const artifacts = { 1: [rerun], 2: [artifact(coverage('test-flink-1', 'flink-integration-tests'))] };
  const same = await executeSelect({
    runs: { contributor: workflows([run(1, { run_attempt: 2 })]) }, artifacts, checks: [recorded],
  });
  assert.deepEqual(same.prs, []);
  assert.equal(same.calls.filter(([kind]) => kind === 'artifacts').length, 0);
  for (const [runs, attempts] of [
    [workflows([run(1, { run_attempt: 3 })]), [3, 1]],
    [workflows([run(1, { run_attempt: 2 })], [run(2, { run_attempt: 2 })]), [2, 2]],
  ]) {
    const result = await executeSelect({ runs: { contributor: runs }, artifacts, checks: [recorded] });
    assert.deepEqual(result.prs, [{ pr: 10, sha, runs: runIds(1, 2, ...attempts) }]);
    assert.equal(result.artifacts.find((item) => item.run_id === 1).artifact_id, rerun.id);
  }
  const { writes } = await executeRecord({
    prs: [{ pr: 10, sha, runs: runIds(1, 2, 3, 1) }], artifacts: [{ pr: 10, fork: 'contributor/hudi' }],
  });
  assert.deepEqual(writes.map((item) => item.external_id), ['hudi-fork-coverage:10:1.3:2.1']);
});
test('names, SHAs, flags, expired and oversized artifacts are validated', async () => {
  const valid = [
    artifact(coverage('test-flink-1', 'flink-integration-tests')),
    artifact(coverage('test-spark-java17-java-tests-part1', 'spark-java-tests'), { size_in_bytes: 50 * MB }),
  ];
  const { artifacts } = await executeSelect({ artifacts: { 1: [
    ...valid,
    artifact('coverage-' + sha + '-test-utilities-utilities'),
    artifact(coverage('test utilities', 'utilities')),
    artifact(coverage('test-utilities', 'Utilities')),
    artifact(coverage('test-utilities', 'utilities') + '.xml'),
    artifact('x' + coverage('test-utilities', 'utilities')),
    artifact(coverage('test-utilities', 'utilities', 'b'.repeat(40))),
    artifact(coverage('test-utilities', 'made-up-flag')),
    artifact(coverage('test-common', 'common-and-other-modules'), { expired: true }),
    artifact(coverage('test-hadoop', 'hadoop-mr-java-client'), { size_in_bytes: 50 * MB + 1 }),
    artifact(coverage('test-scala', 'spark-scala-tests'), { size_in_bytes: undefined }),
    artifact('jacoco'),
  ] } });
  assert.deepEqual(artifacts.map((item) => item.artifact_id), valid.map((item) => item.id));
  assert.deepEqual(artifacts.map((item) => item.flags), ['flink-integration-tests', 'spark-java-tests']);
});
test('a rerun job uploads the same name again and only the newest is kept', async () => {
  const name = coverage('test-utilities', 'utilities');
  const [older, newer] = [artifact(name), artifact(name)];
  const { artifacts } = await executeSelect({ artifacts: { 1: [newer, older] } });
  assert.deepEqual(artifacts.map((item) => item.artifact_id), [newer.id]);
});
test('PRs without matching artifacts are not listed', async () => {
  for (const artifacts of [{ 1: [], 2: [] }, { 1: [artifact('logs')] }]) {
    const result = await executeSelect({ artifacts });
    assert.deepEqual(result.artifacts, []);
    assert.deepEqual(result.prs, []);
  }
});
test('the sweep stops at PRs not updated in the last 24 hours', async () => {
  const old = makePr(11, { updated: now - 25 * 3600 * 1000 });
  const result = await executeSelect({ pulls: [old, pr] });
  assert.deepEqual(result.prs, []);
  assert.deepEqual(result.calls, []);
});
test('forks without the workflow, runs or artifacts are tolerated', async () => {
  const gone = makePr(11, { owner: 'gone' });
  const empty = makePr(12, { owner: 'empty', sha: 'c'.repeat(40) });
  const result = await executeSelect({
    pulls: [gone, empty, pr],
    runs: {
      contributor: workflows(),
      empty: workflows([run(3, { head_sha: empty.head.sha })], [run(4, { head_sha: empty.head.sha })]),
    },
  });
  assert.deepEqual(result.prs, [{ pr: 10, sha, runs: runIds(1, 2) }]);
  assert.deepEqual(result.warnings, []);
});
test('other API errors skip only the failing PR and warn', async () => {
  const broken = makePr(11, { owner: 'broken' });
  const result = await executeSelect({ pulls: [broken, pr], runs: {
    contributor: workflows(), get broken() { throw Object.assign(new Error('API failure'), { status: 500 }); },
  } });
  assert.deepEqual(result.prs, [{ pr: 10, sha, runs: runIds(1, 2) }]);
  assert.deepEqual(result.warnings, ['#11: API failure']);
});
test('a sweep uploads at most 60 reports, and a PR cut by the cap is not recorded', async () => {
  const pulls = [], runs = {}, artifacts = {};
  for (let i = 0; i < 7; i++) {
    const owner = 'contributor' + i;
    const item = makePr(100 + i, { owner, sha: String(i).repeat(40) });
    pulls.push(item);
    // As in Java CI, bot.yml uploads 9 of the 12 reports and java_ci_engines.yml the other 3.
    const head = { head_sha: item.head.sha };
    runs[owner] = workflows([run(10 * i + 1, head)], [run(10 * i + 2, head)]);
    artifacts[10 * i + 1] = Array.from({ length: 9 }, (_, job) =>
      artifact(coverage('job-' + job, 'utilities', item.head.sha)));
    artifacts[10 * i + 2] = Array.from({ length: 3 }, (_, job) =>
      artifact(coverage('job-' + (9 + job), 'integration-tests', item.head.sha)));
  }
  const result = await executeSelect({ pulls, runs, artifacts });
  assert.equal(result.artifacts.length, 60);
  assert.deepEqual(result.prs.map((item) => item.pr), [100, 101, 102, 103, 104]);
  // The fifth PR filled the cap exactly, so no later PR is looked up.
  assert.equal(result.calls.filter(([kind, params]) => kind === 'runs' && params.owner === 'contributor5').length, 0);

  artifacts[11].push(artifact(coverage('job-12', 'utilities', pulls[1].head.sha)));
  const cut = await executeSelect({ pulls, runs, artifacts });
  assert.equal(cut.artifacts.length, 60);
  assert.deepEqual(cut.prs.map((item) => item.pr), [100, 101, 102, 103]);
  assert.equal(cut.artifacts.filter((item) => item.pr === 104).length, 11);
});

async function executeRecord({ prs, artifacts, failing = [] }) {
  const writes = [], failures = [];
  const github = { rest: { checks: {
    create: async (payload) => {
      if (failing.includes(payload.head_sha)) throw new Error('API failure');
      writes.push(payload);
    },
  } } };
  await record(github, { repo: { owner: 'apache', repo: 'hudi' } }, {
    info: () => {}, setFailed: (message) => failures.push(message),
  }, { env: { PRS: JSON.stringify(prs), ARTIFACTS: JSON.stringify(artifacts) } });
  return { writes, failures };
}

test('record creates one completed check per PR', async () => {
  const other = makePr(11, { owner: 'other', sha: 'c'.repeat(40) });
  const { artifacts, prs } = await executeSelect({
    pulls: [pr, other],
    runs: {
      contributor: workflows(),
      other: workflows([run(3, { head_sha: other.head.sha })], [run(4, { head_sha: other.head.sha })]),
    },
    artifacts: {
      1: [artifact(coverage('test-utilities', 'utilities'))],
      2: [artifact(coverage('test-flink-1', 'flink-integration-tests'))],
      3: [artifact(coverage('test-utilities', 'utilities', other.head.sha))],
    },
  });
  const { writes, failures } = await executeRecord({ prs, artifacts });
  assert.deepEqual(failures, []);
  assert.equal(writes.length, 2);
  assert.deepEqual(writes.map((item) => [item.head_sha, item.external_id]), [
    [sha, 'hudi-fork-coverage:10:1.1:2.1'], [other.head.sha, 'hudi-fork-coverage:11:3.1:4.1'],
  ]);
  for (const item of writes) {
    assert.equal(item.owner, 'apache');
    assert.equal(item.name, 'Coverage (fork)');
    assert.equal(item.status, 'completed');
    assert.equal(item.conclusion, 'success');
    assert.match(item.output.summary, /self-reported/);
    assert.match(item.output.summary, /merge queue/);
  }
  assert.equal(writes[0].details_url, 'https://github.com/contributor/hudi/actions/runs/1');
  assert.ok(writes[0].output.summary.includes(
    '\n- [bot.yml run 1, attempt 1](https://github.com/contributor/hudi/actions/runs/1)' +
    '\n- [java_ci_engines.yml run 2, attempt 1](https://github.com/contributor/hudi/actions/runs/2)\n'));
  assert.ok(writes[1].output.summary.includes(
    '\n- [bot.yml run 3, attempt 1](https://github.com/other/hudi/actions/runs/3)' +
    '\n- [java_ci_engines.yml run 4, attempt 1](https://github.com/other/hudi/actions/runs/4)\n'));
});
test('record reports a failed check but still records the other PRs', async () => {
  const prs = [{ pr: 10, sha, runs: runIds(1, 2) }, { pr: 11, sha: 'c'.repeat(40), runs: runIds(3, 4) }];
  const artifacts = [{ pr: 10, fork: 'contributor/hudi' }, { pr: 11, fork: 'other/hudi' }];
  const { writes, failures } = await executeRecord({ prs, artifacts, failing: [sha] });
  assert.deepEqual(writes.map((item) => item.external_id), ['hudi-fork-coverage:11:3.1:4.1']);
  assert.deepEqual(failures, ['#10: API failure']);
});

test('jobs never check out code and pass matrix values only through env', () => {
  const { jobs } = bridgeWorkflow;
  for (const job of Object.values(jobs)) {
    for (const step of job.steps) {
      assert.doesNotMatch(step.uses || '', /actions\/checkout/);
      assert.doesNotMatch(step.run || '', /\$\{\{/);
    }
  }
  assert.equal(jobs.select.if, "github.repository == 'apache/hudi'");
  assert.deepEqual(bridgeWorkflow.permissions, {});
  assert.equal(jobs.upload.strategy['fail-fast'], false);
  assert.equal(jobs.upload.strategy['max-parallel'], 4);
});
