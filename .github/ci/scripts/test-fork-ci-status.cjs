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

// Adapted from apache/arrow-rs .github/ci/scripts/test-fork-ci-status.cjs.
const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const { readFileSync } = fs;
const os = require('node:os');
const path = require('node:path');
const { test } = require('node:test');

function workflow(name) {
  return JSON.parse(execFileSync('python3', [
    '-c', 'import json, sys, yaml; print(json.dumps(yaml.safe_load(sys.stdin)))',
  ], {
    input: readFileSync(path.join(__dirname, '../../workflows', name)),
    encoding: 'utf8',
  }));
}
const bridgeWorkflow = workflow('fork_ci_status.yml');
// The Java CI workflow the bridge mirrors, with the id of its aggregate job and of the transitional
// job that reports the former Java CI Engines context.
const ci = {
  file: 'bot.yml', name: 'Java CI', aggregate: 'java-ci-result', transitional: 'java-ci-engines-result',
  workflow: workflow('bot.yml'),
};
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const bridge = new AsyncFunction('github', 'context', 'core', 'Date',
  bridgeWorkflow.jobs.bridge.steps[0].with.script);
const now = Date.UTC(2026, 9, 20, 12);
class Clock extends Date { static now() { return now; } }
const sha = 'a'.repeat(40);
const pr = {
  number: 10, state: 'open', updated_at: new Date(now).toISOString(),
  head: {
    sha, ref: 'feature',
    repo: { owner: { login: 'contributor' }, name: 'hudi', full_name: 'contributor/hudi' },
  },
  base: { ref: 'master' },
};
const run = (id = 1, extra = {}) => ({
  id, event: 'push', head_sha: sha, head_branch: 'feature', run_attempt: 1,
  status: 'completed', conclusion: 'success',
  html_url: 'https://github.com/contributor/hudi/actions/runs/' + id,
  ...extra,
});

// runs and workflowState describe the fork's bot.yml.
async function execute({
  event = 'schedule', checks = [], runs = [run()], workflowState = 'active',
  recent = [pr], slice = [pr], candidates = [pr], repo = 'hudi',
  owner = 'apache', runError, count = 1, notification = {}, checkErrors = {},
} = {}) {
  const writes = [], calls = [], failures = [];
  let sweepPage;
  const github = { rest: { checks: {}, actions: {}, pulls: {} } };
  github.rest.checks.listForRef = async () => {};
  github.rest.pulls.list = async (params) => {
    calls.push(['pulls', params]);
    if (params.per_page === 1) return {
      data: count ? [pr] : [],
      headers: count ? { link: '<https://api.github.com/pulls?per_page=1&page=' + count + '>; rel="last"' } : {},
    };
    assert.equal(params.per_page, 25);
    sweepPage = params.page;
    return { data: slice };
  };
  github.rest.actions.listWorkflowRuns = async (params) => {
    calls.push(['runs', params]);
    assert.equal(params.owner, 'contributor');
    assert.equal(params.head_sha, sha);
    assert.equal(params.branch, 'feature');
    assert.equal(params.workflow_id, 'bot.yml');
    if (runError) throw Object.assign(new Error('API failure'), { status: runError });
    return { data: { workflow_runs: runs } };
  };
  github.rest.actions.getWorkflow = async (params) => {
    assert.equal(params.workflow_id, 'bot.yml');
    if (workflowState === null) throw Object.assign(new Error('Not Found'), { status: 404 });
    return { data: { state: workflowState } };
  };
  github.rest.checks.create = async (payload) => writes.push({ method: 'create', ...payload });
  github.rest.checks.update = async (payload) => writes.push({ method: 'update', ...payload });
  github.paginate = async (fn, params) => {
    if (fn === github.rest.checks.listForRef) {
      calls.push(['checks', params]);
      if (checkErrors[params.ref]) throw new Error(checkErrors[params.ref]);
      return checks;
    }
    assert.equal(fn, github.rest.pulls.list);
    assert.equal(params.head, 'contributor:feature');
    return candidates;
  };
  github.paginate.iterator = async function* () {
    for (let i = 0; i < recent.length; i += 100) yield { data: recent.slice(i, i + 100) };
  };
  await bridge(github, {
    repo: { owner, repo }, eventName: event,
    payload: { workflow_run: {
      event: 'pull_request', head_sha: sha, head_branch: 'feature',
      head_repository: pr.head.repo, pull_requests: [], ...notification,
    } },
  }, { info: () => {}, setFailed: (message) => failures.push(message) }, Clock);
  return { writes, calls, sweepPage, failures };
}
async function existing(runValue = run()) {
  const { writes } = await execute({ runs: [runValue] });
  return {
    id: 100, app: { slug: 'github-actions' }, external_id: 'hudi-fork-ci:10',
    ...writes[0], conclusion: writes[0].conclusion || null,
  };
}

test('CI completion discovers a fork PR even when the event PR list is empty', async () => {
  const { writes } = await execute({ event: 'workflow_run' });
  assert.equal(writes.length, 1);
  assert.equal(writes[0].head_sha, sha);
  assert.equal(writes[0].name, 'CI (fork)');
  assert.equal(writes[0].conclusion, 'success');
  assert.equal(writes[0].details_url, run().html_url);
});
test('notifications ignore stale branch heads, release PRs, closed PRs and deleted forks', async () => {
  for (const candidate of [
    { ...pr, head: { ...pr.head, sha: 'b'.repeat(40) } },
    { ...pr, base: { ref: 'release-1.2' } },
    { ...pr, state: 'closed' },
    { ...pr, head: { ...pr.head, repo: null } },
  ]) {
    assert.deepEqual((await execute({ event: 'workflow_run', candidates: [candidate] })).writes, []);
  }
});
test('upstream pushes and merge-queue completions cannot create fork checks', async () => {
  for (const event of ['push', 'merge_group']) {
    assert.deepEqual((await execute({ event: 'workflow_run', notification: { event } })).writes, []);
  }
});
test('the bridge does nothing when copied into a fork', async () => {
  const result = await execute({ owner: 'contributor' });
  assert.deepEqual(result.calls, []);
  assert.deepEqual(result.writes, []);
});
test('only runs for the exact branch head on push/manual events are mirrored', async () => {
  const result = await execute({ runs: [
    run(8, { head_sha: 'b'.repeat(40) }),
    run(7, { head_branch: 'other' }),
    run(6, { event: 'pull_request' }),
    run(5, { event: 'workflow_dispatch' }),
    run(1),
  ] });
  assert.equal(result.writes[0].details_url, run(5).html_url);
});
for (const [state, expected, title] of [
  ['active', 'neutral', 'No fork CI run for this commit'],
  ['disabled_manually', 'action_required', 'Enable CI on your fork'],
  ['disabled_fork', 'action_required', 'Enable CI on your fork'],
  [null, 'action_required', 'Enable CI on your fork'],
]) {
  test('missing run reports workflow state ' + state, async () => {
    const { writes } = await execute({ runs: [], workflowState: state });
    assert.equal(writes[0].conclusion, expected);
    assert.equal(writes[0].output.title, title);
    assert.equal(writes[0].details_url, 'https://github.com/contributor/hudi/actions');
    assert.match(writes[0].output.summary, /^No Java CI run was found for /);
    assert.match(writes[0].output.summary, /Enable workflows/);
    assert.match(writes[0].output.summary, /only docs or images is skipped by the path filter/);
    assert.deepEqual(JSON.parse(writes[0].output.text).runs.map((item) => item.run_id), [null]);
  });
}
test('a missing workflow endpoint is handled as missing CI', async () => {
  const { writes } = await execute({ runError: 404, workflowState: null });
  assert.equal(writes[0].conclusion, 'action_required');
});
test('permission, rate limit and server errors are not reported as missing CI', async () => {
  for (const runError of [403, 429, 500]) {
    const result = await execute({ runError });
    assert.deepEqual(result.writes, []);
    assert.deepEqual(result.failures, ['#10: API failure']);
    await assert.rejects(execute({ event: 'workflow_run', runError }), /API failure/);
  }
});
for (const conclusion of [
  'failure', 'cancelled', 'timed_out', 'action_required', 'neutral', 'skipped', 'startup_failure', 'stale',
]) {
  test('mirrors ' + conclusion + ' without reporting success', async () => {
    const { writes } = await execute({ runs: [run(1, { conclusion })] });
    assert.equal(writes[0].conclusion,
      ['startup_failure', 'stale'].includes(conclusion) ? 'failure' : conclusion);
  });
}
test('a stale run updates an existing check with failure, not the reserved conclusion', async () => {
  const { writes } = await execute({
    checks: [await existing()], runs: [run(1, { conclusion: 'stale' })],
  });
  assert.equal(writes[0].method, 'update');
  assert.equal(writes[0].conclusion, 'failure');
});
for (const source of ['recent', 'slice']) {
  test('the sweep continues after errors in the ' + source + ' PRs and reports every failure', async () => {
    const broken = ['b', 'c'].map((letter, index) => ({
      ...pr, number: 20 + index, head: { ...pr.head, sha: letter.repeat(40) },
    }));
    const result = await execute({
      recent: [], slice: [], [source]: [...broken, pr], count: 3,
      checkErrors: {
        [broken[0].head.sha]: 'First API failure',
        [broken[1].head.sha]: 'Second API failure',
      },
    });
    assert.equal(result.writes.length, 1);
    assert.equal(result.writes[0].external_id, 'hudi-fork-ci:10');
    assert.equal(result.writes[0].conclusion, 'success');
    assert.deepEqual(result.failures, ['#20: First API failure\n#21: Second API failure']);
  });
}
test('a rerun of the same workflow run updates the existing check', async () => {
  const check = await existing(run(1, { conclusion: 'failure' }));
  const { writes } = await execute({ checks: [check], runs: [run(1, { run_attempt: 2 })] });
  assert.equal(writes[0].method, 'update');
  assert.equal(writes[0].check_run_id, 100);
  assert.equal(writes[0].conclusion, 'success');
  assert.deepEqual(JSON.parse(writes[0].output.text).runs.map((item) => item.run_attempt), [2]);
});
test('a manual run can replace a failed push run', async () => {
  const check = await existing(run(1, { conclusion: 'failure' }));
  const { writes } = await execute({
    checks: [check], runs: [run(2, { event: 'workflow_dispatch' }), run(1)],
  });
  assert.equal(writes[0].details_url, run(2).html_url);
});
test('a deleted run clears its stale success result', async () => {
  const { writes } = await execute({ checks: [await existing()], runs: [] });
  assert.equal(writes[0].method, 'update');
  assert.equal(writes[0].conclusion, 'neutral');
});
test('unchanged results do not consume check write requests', async () => {
  const check = await existing();
  // GitHub can replace details_url with the check's own URL.
  for (const details_url of [check.details_url, 'https://github.com/apache/hudi/runs/100']) {
    assert.deepEqual((await execute({ checks: [{ ...check, details_url }] })).writes, []);
  }
});
test('a same-name check from another app or PR is not overwritten', async () => {
  const check = await existing();
  for (const foreign of [
    { ...check, app: { slug: 'other-app' } },
    { ...check, external_id: 'hudi-fork-ci:20' },
  ]) {
    assert.equal((await execute({ checks: [foreign] })).writes[0].method, 'create');
  }
});
test('recent successful checks wait for the rotating sweep', async () => {
  const { writes, calls } = await execute({ checks: [await existing()], slice: [] });
  assert.deepEqual(writes, []);
  assert.equal(calls.filter(([kind]) => kind === 'runs').length, 0);
});
test('a successful rerun of a failed run is picked up outside the sweep slice', async () => {
  const check = await existing(run(1, { conclusion: 'failure' }));
  assert.equal(check.conclusion, 'failure');
  const { writes } = await execute({ checks: [check], slice: [], runs: [run(1, { run_attempt: 2 })] });
  assert.equal(writes[0].conclusion, 'success');
});
test('late runs are recovered outside the sweep slice', async () => {
  const missing = await execute({ runs: [] });
  const check = { ...await existing(), ...missing.writes[0] };
  const { writes } = await execute({ checks: [check], slice: [] });
  assert.equal(writes[0].conclusion, 'success');
});
for (const status of ['queued', 'in_progress']) {
  test('polls recent ' + status + ' runs outside the sweep slice', async () => {
    const check = await existing(run(1, { status, conclusion: null }));
    const { writes } = await execute({ checks: [check], slice: [] });
    assert.equal(writes[0].conclusion, 'success');
  });
}
test('the sweep reaches old PRs beyond 500 open PRs', async () => {
  const old = { ...pr, updated_at: new Date(now - 24 * 3600 * 1000).toISOString() };
  const { writes, sweepPage } = await execute({ count: 600, recent: [old], slice: [old] });
  assert.equal(sweepPage, Math.floor(now / (15 * 60 * 1000)) % 24 + 1);
  assert.equal(writes.length, 1);
});
test('an active PR appearing in both lists is polled once', async () => {
  const { calls } = await execute();
  assert.deepEqual(calls.filter(([kind]) => kind === 'runs').map(([, params]) => params.workflow_id),
    ['bot.yml']);
});
test('no open PRs means no fork API calls', async () => {
  const result = await execute({ count: 0 });
  assert.equal(result.calls.length, 1);
  assert.deepEqual(result.writes, []);
});

const runUrl = (id) => 'https://github.com/contributor/hudi/actions/runs/' + id;
test('a successful run reports success, links the run and records it', async () => {
  const { writes } = await execute({ runs: [run(1, { run_attempt: 3 })] });
  assert.equal(writes[0].status, 'completed');
  assert.equal(writes[0].conclusion, 'success');
  assert.equal(writes[0].details_url, runUrl(1));
  assert.ok(writes[0].output.summary.includes('- [Java CI](' + runUrl(1) + '): success'));
  assert.deepEqual(JSON.parse(writes[0].output.text).runs, [
    { workflow: 'bot.yml', run_id: 1, run_attempt: 3 },
  ]);
});
test('the check stays in progress until the run completes', async () => {
  for (const status of ['queued', 'in_progress']) {
    const { writes } = await execute({ runs: [run(1, { status, conclusion: null })] });
    assert.equal(writes[0].status, 'in_progress');
    assert.equal(writes[0].conclusion, undefined);
    assert.match(writes[0].output.summary, new RegExp(': ' + status + '$', 'm'));
  }
});


// Evaluates the bot.yml expressions as JavaScript, which shares their syntax for the operators used
// there. needs.*.result becomes the list of the job results, and needs.<job id> an index, since a
// job id may contain hyphens.
function evaluate(expression, scope) {
  const body = expression.replace(/needs\.\*\.result/g, 'Object.values(needs).map((job) => job.result)')
    .replace(/needs\.([A-Za-z_][\w-]*)/g, "needs['$1']");
  return new Function(...Object.keys(scope), 'return (' + body + ');')(...Object.values(scope));
}
function interpolate(value, scope) {
  const whole = /^\s*\$\{\{((?:(?!\}\})[\s\S])*)\}\}\s*$/.exec(value);
  if (whole) return evaluate(whole[1], scope);
  return value.replace(/\$\{\{((?:(?!\}\})[\s\S])*)\}\}/g, (_, expression) => evaluate(expression, scope));
}
const functions = {
  always: () => true,
  cancelled: () => false,
  startsWith: (text, prefix) => text.startsWith(prefix),
  contains: (list, item) => list.includes(item),
};
const withName = (github) => ({ ...github, workflow: ci.name });
const group = (github) => interpolate(ci.workflow.concurrency.group, { github: withName(github) });
const cancels = (github) => interpolate(ci.workflow.concurrency['cancel-in-progress'], { github: withName(github) });
// The `changes` job runs on merge_group only, so it is skipped and has no outputs by default.
const skippedChanges = { changes: { result: 'skipped', outputs: {} } };
const jobIf = (job, github, needs = skippedChanges) =>
  interpolate(ci.workflow.jobs[job].if, { github: withName(github), needs, ...functions });
const testJobs = Object.keys(ci.workflow.jobs).filter((job) =>
  ![ci.aggregate, ci.transitional, 'changes'].includes(job));
// GitHub expressions resolve absent properties to an empty string.
function context({
  event_name, repository = 'apache/hudi', ref = 'refs/heads/feature', sha = 'first',
  number = '', head = '', base_ref = '',
}) {
  return {
    event_name, repository, ref, sha, base_ref,
    event: { pull_request: { number, head: { repo: { full_name: head } } } },
  };
}
const forkPr = context({
  event_name: 'pull_request', ref: 'refs/pull/1/merge', number: 1, head: 'contributor/hudi', base_ref: 'master',
});
const upstreamPr = { ...forkPr, event: { pull_request: { number: 1, head: { repo: { full_name: 'apache/hudi' } } } } };
const queue = context({ event_name: 'merge_group', ref: 'refs/heads/gh-readonly-queue/master/pr-1-first' });
const forkPush = context({ event_name: 'push', repository: 'contributor/hudi' });
const upstreamPush = (branch) => context({ event_name: 'push', ref: 'refs/heads/' + branch });

const FORK_PR = "github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name != github.repository && github.base_ref == 'master'";
const UPSTREAM_OTHER_PUSH = "github.event_name == 'push' && github.repository == 'apache/hudi' && github.ref != 'refs/heads/master' && !startsWith(github.ref, 'refs/heads/release-') && github.ref != 'refs/heads/branch-0.x'";
const DOCS_ONLY = "needs.changes.outputs.docs_only == 'true'";
// PyYAML resolves the `*route` aliases, so every test job shows the expanded condition.
test('every test job carries the route condition, and the aggregate job does not', () => {
  assert.ok(testJobs.length > 1);
  for (const job of testJobs) {
    assert.equal(ci.workflow.jobs[job].if,
      '${{ !cancelled() && !(' + FORK_PR + ') && !(' + UPSTREAM_OTHER_PUSH + ') && ' + DOCS_ONLY.replace('==', '!=') + ' }}',
      job);
    assert.equal(ci.workflow.jobs[job].needs, 'changes', job);
  }
  const aggregate = ci.workflow.jobs[ci.aggregate];
  assert.equal(aggregate.if, '${{ always() && !(' + UPSTREAM_OTHER_PUSH + ') }}');
  assert.equal(aggregate.env.FORK_PR, '${{ ' + FORK_PR + ' }}');
  // A test job missing from needs would never fail the aggregate.
  assert.deepEqual([...aggregate.needs].sort(), [...testJobs].sort());
});
// The ASF Actions policy caps the jobs running at once in a workflow at 20, across all matrices.
test('the matrix-expanded test jobs stay within the ASF cap of 20 jobs at once', () => {
  let total = 0;
  for (const job of testJobs) {
    const matrix = ci.workflow.jobs[job].strategy?.matrix;
    const axes = Object.keys(matrix || {}).filter((key) => !['include', 'exclude'].includes(key));
    if (axes.length) {
      // Combining axes with include or exclude needs GitHub's matching rules; extend this count first.
      assert.ok(!('include' in matrix) && !('exclude' in matrix), job);
      total += axes.reduce((product, axis) => product * matrix[axis].length, 1);
    } else {
      total += matrix?.include ? matrix.include.length : 1;
    }
  }
  assert.ok(total <= 20, 'test job instances: ' + total);
  // The aggregate job's comment states the current total.
  const stated = /test jobs currently total (\d+) instances/.exec(
    readFileSync(path.join(__dirname, '../../workflows/bot.yml'), 'utf8'));
  assert.equal(Number(stated?.[1]), total);
});

// [case, context, whether the test jobs run, whether the aggregate jobs run, the `changes` job]
for (const [name, github, tests, aggregate, needs] of [
  ['fork PR to master', forkPr, false, true],
  ['fork PR to release-1.2', { ...forkPr, base_ref: 'release-1.2' }, true, true],
  ['fork PR to branch-0.x', { ...forkPr, base_ref: 'branch-0.x' }, true, true],
  ['same-repository PR to master', upstreamPr, true, true],
  ['merge queue', queue, true, true],
  ['merge queue with a docs-only change', queue, false, true, { changes: { result: 'success', outputs: { docs_only: 'true' } } }],
  ['merge queue with a code change', queue, true, true, { changes: { result: 'success', outputs: { docs_only: 'false' } } }],
  // A failed selector has no outputs, so the queue tests everything.
  ['merge queue with a failed selector', queue, true, true, { changes: { result: 'failure', outputs: {} } }],
  // Only the event_name guard keeps a queue run from matching FORK_PR if base_ref were set.
  ['merge queue with a base_ref', { ...queue, base_ref: 'master' }, true, true],
  ['upstream push to master', upstreamPush('master'), true, true],
  ['upstream push to release-1.2', upstreamPush('release-1.2'), true, true],
  ['upstream push to branch-0.x', upstreamPush('branch-0.x'), true, true],
  ['upstream push to another branch', upstreamPush('feature'), false, false],
  ['fork push to any branch', forkPush, true, true],
  ['manual run on an upstream branch', context({ event_name: 'workflow_dispatch' }), true, true],
]) {
  test('routes ' + name, () => {
    for (const job of testJobs) assert.equal(jobIf(job, github, needs), tests, job);
    assert.equal(jobIf(ci.aggregate, github, needs), aggregate, ci.aggregate);
    assert.equal(jobIf(ci.transitional, github, needs), aggregate, ci.transitional);
    // The selector runs on merge_group only; GitHub evaluates an `if` without ${{ }} as an expression.
    assert.equal(evaluate(ci.workflow.jobs.changes.if, { github }), github.event_name === 'merge_group');
  });
}
function aggregateFails(github, results, overrides = {}) {
  const job = ci.workflow.jobs[ci.aggregate];
  const env = { FORK_PR: String(interpolate(job.env.FORK_PR, { github })) };
  const step = job.steps.find((item) => item.name === 'Fail unless every job succeeded');
  const needs = Object.fromEntries(results.map((result, index) => ['job-' + index, { result }]));
  return interpolate(step.if, { github, env, needs, ...functions, ...overrides });
}
test('the aggregate job accepts skipped test jobs only on fork PRs to master', () => {
  assert.equal(aggregateFails(forkPr, ['skipped', 'skipped', 'skipped']), false);
  for (const results of [['skipped', 'failure'], ['cancelled', 'skipped'], ['success', 'skipped'], ['success', 'success']]) {
    assert.equal(aggregateFails(forkPr, results), true, results.join());
  }
  for (const github of [upstreamPr, forkPush, upstreamPush('master'), { ...forkPr, base_ref: 'release-1.2' }]) {
    assert.equal(aggregateFails(github, ['success', 'success', 'success']), false);
    for (const results of [['skipped', 'skipped'], ['success', 'skipped'], ['failure', 'success'], ['success', 'cancelled']]) {
      assert.equal(aggregateFails(github, results), true, results.join());
    }
  }
});
// On merge_group the route skips a test job only for DOCS_ONLY, which skips them all.
test('the aggregate job accepts a merge queue run in which every test job skipped', () => {
  for (const results of [['success', 'success', 'success'], ['skipped', 'skipped', 'skipped']]) {
    assert.equal(aggregateFails(queue, results), false, results.join());
  }
  for (const results of [['success', 'skipped'], ['skipped', 'failure'], ['skipped', 'cancelled'], ['failure', 'success'], ['success', 'cancelled']]) {
    assert.equal(aggregateFails(queue, results), true, results.join());
  }
  // A queue run cancelled before its test jobs started is not docs-only: the test jobs still
  // pending at the cancel report cancelled, not skipped.
  assert.equal(aggregateFails(queue, ['cancelled', 'cancelled', 'cancelled']), true);
});
// The docs-only skip relies on every queue entry passing its own run. Under HEADGREEN only the
// head of a group must pass, so a docs-only entry there would merge the code entries ahead of it.
test('the merge queue requires every entry to pass, which the docs-only skip relies on', () => {
  const asf = JSON.parse(execFileSync('python3', [
    '-c', 'import json, sys, yaml; print(json.dumps(yaml.safe_load(sys.stdin)))',
  ], { input: readFileSync(path.join(__dirname, '../../../.asf.yaml')), encoding: 'utf8' }));
  const queues = asf.github.rulesets.flatMap((ruleset) => ruleset.rules)
    .filter((rule) => rule.type === 'merge_queue');
  assert.equal(queues.length, 1);
  assert.equal(queues[0].parameters.grouping_strategy, 'ALLGREEN',
    'bot.yml skips the tests of docs-only queue entries, which is safe only under ALLGREEN');
});
test('the aggregate job links the fork run of bot.yml on fork PRs', () => {
  const step = ci.workflow.jobs[ci.aggregate].steps.find((item) => item.name === 'Link to fork CI');
  assert.equal(evaluate(step.if, { env: { FORK_PR: 'true' } }), true);
  assert.equal(evaluate(step.if, { env: { FORK_PR: 'false' } }), false);
  assert.ok(step.run.includes('/actions/workflows/' + ci.file + ')'));
  assert.doesNotMatch(step.run, /\$\{\{/);
});
// Branch protection required `Java CI Engines result` before Java CI Engines was folded into
// bot.yml, so a transitional job reports that context with the result of the aggregate job.
test('the transitional job reports the result of the aggregate job as Java CI Engines result', () => {
  const job = ci.workflow.jobs[ci.transitional];
  const aggregate = ci.workflow.jobs[ci.aggregate];
  assert.equal(job.name, 'Java CI Engines result');
  assert.equal(job.needs, ci.aggregate);
  assert.equal(job.if, aggregate.if);
  assert.equal(job['runs-on'], 'ubuntu-slim');
  assert.deepEqual(job.permissions, {});
  assert.equal(job.steps.length, 1);
  const [step] = job.steps;
  for (const result of ['success', 'failure', 'skipped', 'cancelled']) {
    const needs = { [ci.aggregate]: { result } };
    assert.equal(interpolate(step.if, { needs, ...functions }), result !== 'success', result);
  }
  assert.match(step.run, /::error::/);
  assert.match(step.run, /exit 1/);
  assert.doesNotMatch(step.run, /\$\{\{/);
});

// The docs-only selector, the `changes` job.
const changesJob = ci.workflow.jobs.changes;
const changesStep = changesJob.steps.find((item) => item.id === 'diff');
test('the changes job runs on merge_group only', () => {
  assert.equal(changesJob.if, "github.event_name == 'merge_group'");
  assert.equal(changesJob.outputs.docs_only, '${{ steps.diff.outputs.docs_only }}');
  // Expressions reach the script only through env.
  assert.doesNotMatch(changesStep.run, /\$\{\{/);
});
test('the changes job excludes exactly what the push filter ignores', () => {
  const excluded = [...changesStep.run.matchAll(/':\(([^)]*)\)([^']*)'/g)].map(([pathspec]) => pathspec);
  assert.ok(excluded.length > 1);
  // PyYAML reads the key `on` as the boolean true.
  const ignored = ci.workflow.true.push.paths.filter((pattern) => pattern.startsWith('!')).map((pattern) => {
    const glob = pattern.slice(1);
    // GitHub's `**.md` matches at any depth; git's `**/*.md` does too, including the root.
    if (/^\*\*\.[a-z]+$/.test(glob)) return "':(exclude,glob)**/*" + glob.slice(2) + "'";
    // Without the glob magic a pathspec without wildcards matches that path from the root only.
    assert.match(glob, /^[^*?[]+$/, pattern);
    return "':(exclude)" + glob + "'";
  });
  assert.deepEqual(excluded, ignored);
});
// Runs the selector script against a local repository served over file://, as the merge queue
// runs it against apache/hudi.
test('the changes job reports docs_only for diffs of only docs and images', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'hudi-changes-'));
  const env = { ...process.env, GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: path.join(tmp, 'gitconfig') };
  const git = (...args) => execFileSync('git', [
    '-c', 'user.name=ci', '-c', 'user.email=ci@example.com', '-c', 'init.defaultBranch=master', ...args,
  ], { cwd: source, env, encoding: 'utf8', stdio: 'pipe' }).trim();
  const write = (file, content = file) => {
    fs.mkdirSync(path.dirname(path.join(source, file)), { recursive: true });
    fs.writeFileSync(path.join(source, file), content + '\n');
  };
  const source = path.join(tmp, 'apache', 'hudi.git');
  const select = (base, head) => {
    const work = fs.mkdtempSync(path.join(tmp, 'run-'));
    const output = path.join(tmp, path.basename(work) + '.output');
    const summary = path.join(tmp, path.basename(work) + '.summary');
    execFileSync('bash', ['-c', changesStep.run], {
      cwd: work, stdio: 'pipe', env: {
        ...env, GITHUB_SERVER_URL: 'file://' + tmp, GITHUB_REPOSITORY: 'apache/hudi',
        BASE_SHA: base, HEAD_SHA: head, GITHUB_OUTPUT: output, GITHUB_STEP_SUMMARY: summary,
      },
    });
    return { output: readFileSync(output, 'utf8'), summary: readFileSync(summary, 'utf8') };
  };
  try {
    fs.mkdirSync(source, { recursive: true });
    git('init', '-q');
    git('config', 'uploadpack.allowFilter', 'true');
    git('config', 'uploadpack.allowAnySHA1InWant', 'true');
    for (const file of ['README.md', 'notes.md', 'sub/A.java', '.gitignore']) write(file);
    git('add', '-A');
    git('commit', '-qm', 'base');
    const base = git('rev-parse', 'HEAD');
    // [case, change, whether docs-only, files the summary lists]
    for (const [name, change, docsOnly, listed = []] of [
      ['root README.md', () => write('README.md', 'changed'), true],
      ['nested markdown', () => write('rfc/x/y.md'), true],
      ['image', () => write('docs/images/arch.png'), true],
      ['root .gitignore', () => write('.gitignore', 'changed'), true],
      ['nested .gitignore', () => write('sub/.gitignore'), false, ['sub/.gitignore']],
      ['yml file', () => write('docker/compose.yml'), false, ['docker/compose.yml']],
      ['java file', () => write('sub/A.java', 'changed'), false, ['sub/A.java']],
      ['rename of .md to .java', () => git('mv', 'notes.md', 'Notes.java'), false, ['Notes.java']],
      ['docs and one java file', () => { write('docs/guide.md'); write('sub/A.java', 'changed'); }, false, ['sub/A.java']],
      ['more than 50 files', () => { for (let i = 0; i < 52; i++) write('gen/F' + i + '.java'); }, false, ['(52)', 'gen/F0.java', 'gen/F7.java', '... and 2 more']],
    ]) {
      git('checkout', '-q', '--detach', base);
      change();
      git('add', '-A');
      git('commit', '-qm', name);
      const { output, summary } = select(base, git('rev-parse', 'HEAD'));
      assert.equal(output, 'docs_only=' + docsOnly + '\n', name);
      for (const text of listed) assert.ok(summary.includes(text), name + ': ' + text + ' in\n' + summary);
      assert.doesNotMatch(summary, /\.md$|notes|guide|gen\/F[89]\.java/m, name);
    }
    // A failed fetch fails the job, which leaves docs_only empty, so the test jobs run.
    assert.throws(() => select(base, 'f'.repeat(40)));
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('successive fork pushes share a cancellation group, separate branches do not', () => {
  assert.equal(cancels(forkPush), true);
  assert.equal(group(forkPush), group({ ...forkPush, sha: 'second' }));
  assert.notEqual(group(forkPush), group({ ...forkPush, ref: 'refs/heads/other' }));
});
// Unlike arrow-rs, event_name keeps manual runs apart from push runs, on forks as upstream.
test('fork manual runs neither replace nor cancel push runs', () => {
  const manual = { ...forkPush, event_name: 'workflow_dispatch' };
  assert.equal(cancels(manual), false);
  assert.notEqual(group(forkPush), group(manual));
  assert.notEqual(group(manual), group({ ...manual, ref: 'refs/heads/other' }));
  assert.notEqual(group(manual), group(forkPr));
});
test('upstream pushes and merge queue runs remain separate per commit and never cancel', () => {
  const push = upstreamPush('master');
  assert.equal(cancels(push), false);
  assert.notEqual(group(push), group({ ...push, sha: 'second' }));
  // Each queue entry has a ref of its own, which names the PR and the commit.
  assert.equal(cancels(queue), false);
  assert.notEqual(group(queue), group({
    ...queue, ref: 'refs/heads/gh-readonly-queue/master/pr-2-second', sha: 'second',
  }));
  for (const event_name of ['workflow_dispatch', 'merge_group']) {
    assert.notEqual(group(push), group({ ...push, event_name }));
  }
});
test('same-named branches from different fork PRs cannot cancel each other', () => {
  assert.equal(cancels(forkPr), true);
  assert.notEqual(group(forkPr), group({ ...forkPr, event: { pull_request: { number: 2 } } }));
});
