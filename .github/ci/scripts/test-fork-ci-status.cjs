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
const { readFileSync } = require('node:fs');
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
// The two Java CI workflows the bridge mirrors, each with the id of its aggregate job.
const javaCi = [
  { file: 'bot.yml', name: 'Java CI', aggregate: 'java-ci-result' },
  { file: 'java_ci_engines.yml', name: 'Java CI Engines', aggregate: 'java-ci-engines-result' },
].map((item) => ({ ...item, workflow: workflow(item.file) }));
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

// runs and workflowState describe bot.yml; java_ci_engines.yml has the same unless engineRuns or
// engineWorkflowState says otherwise.
async function execute({
  event = 'schedule', checks = [], runs = [run()], workflowState = 'active',
  engineRuns = runs, engineWorkflowState = workflowState,
  recent = [pr], slice = [pr], candidates = [pr], repo = 'hudi',
  owner = 'apache', runError, count = 1, notification = {}, checkErrors = {},
} = {}) {
  const writes = [], calls = [], failures = [];
  let sweepPage;
  const github = { rest: { checks: {}, actions: {}, pulls: {} } };
  const byWorkflow = (id, bot, engines) => {
    assert.ok(['bot.yml', 'java_ci_engines.yml'].includes(id), id);
    return id === 'bot.yml' ? bot : engines;
  };
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
    if (runError) throw Object.assign(new Error('API failure'), { status: runError });
    return { data: { workflow_runs: byWorkflow(params.workflow_id, runs, engineRuns) } };
  };
  github.rest.actions.getWorkflow = async (params) => {
    const state = byWorkflow(params.workflow_id, workflowState, engineWorkflowState);
    if (state === null) throw Object.assign(new Error('Not Found'), { status: 404 });
    return { data: { state } };
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
for (const [state, expected] of [
  ['active', 'neutral'], ['disabled_fork', 'action_required'], [null, 'action_required'],
]) {
  test('missing run reports workflow state ' + state, async () => {
    const { writes } = await execute({ runs: [], workflowState: state });
    assert.equal(writes[0].conclusion, expected);
    assert.match(writes[0].output.summary, /Enable workflows/);
    assert.match(writes[0].output.summary, /only docs or images is skipped by the path filter/);
    assert.deepEqual(JSON.parse(writes[0].output.text).runs.map((item) => item.run_id), [null, null]);
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
for (const conclusion of ['failure', 'cancelled', 'timed_out', 'startup_failure', 'stale']) {
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
  assert.deepEqual(JSON.parse(writes[0].output.text).runs.map((item) => item.run_attempt), [2, 2]);
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
test('recent completed checks wait for the rotating sweep', async () => {
  const { writes, calls } = await execute({ checks: [await existing()], slice: [] });
  assert.deepEqual(writes, []);
  assert.equal(calls.filter(([kind]) => kind === 'runs').length, 0);
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
    ['bot.yml', 'java_ci_engines.yml']);
});
test('no open PRs means no fork API calls', async () => {
  const result = await execute({ count: 0 });
  assert.equal(result.calls.length, 1);
  assert.deepEqual(result.writes, []);
});

// One check combines the fork's runs of both workflows.
const runUrl = (id) => 'https://github.com/contributor/hudi/actions/runs/' + id;
test('two successful runs report success, link both and record both', async () => {
  const { writes } = await execute({ runs: [run(1)], engineRuns: [run(2, { run_attempt: 3 })] });
  assert.equal(writes[0].status, 'completed');
  assert.equal(writes[0].conclusion, 'success');
  assert.equal(writes[0].details_url, runUrl(1));
  assert.ok(writes[0].output.summary.includes('- [Java CI](' + runUrl(1) + '): success'));
  assert.ok(writes[0].output.summary.includes('- [Java CI Engines](' + runUrl(2) + '): success'));
  assert.deepEqual(JSON.parse(writes[0].output.text).runs, [
    { workflow: 'bot.yml', run_id: 1, run_attempt: 1 },
    { workflow: 'java_ci_engines.yml', run_id: 2, run_attempt: 3 },
  ]);
});
test('one failed run fails the combined check, whichever workflow it is', async () => {
  for (const [bot, engines] of [['failure', 'success'], ['success', 'failure']]) {
    const { writes } = await execute({
      runs: [run(1, { conclusion: bot })], engineRuns: [run(2, { conclusion: engines })],
    });
    assert.equal(writes[0].conclusion, 'failure', bot + ',' + engines);
    assert.equal(writes[0].details_url, runUrl(1));
  }
});
test('the worst conclusion of the two runs wins', async () => {
  const order = ['failure', 'timed_out', 'cancelled', 'action_required', 'neutral', 'skipped', 'success'];
  for (const [index, worse] of order.entries()) {
    for (const better of order.slice(index)) {
      for (const [bot, engines] of [[worse, better], [better, worse]]) {
        const { writes } = await execute({
          runs: [run(1, { conclusion: bot })], engineRuns: [run(2, { conclusion: engines })],
        });
        assert.equal(writes[0].conclusion, worse, bot + ',' + engines);
      }
    }
  }
});
test('the combined check stays in progress until both runs complete', async () => {
  for (const status of ['queued', 'in_progress']) {
    for (const [bot, engines] of [
      [run(1), run(2, { status, conclusion: null })],
      [run(1, { status, conclusion: null }), run(2, { conclusion: 'failure' })],
    ]) {
      const { writes } = await execute({ runs: [bot], engineRuns: [engines] });
      assert.equal(writes[0].status, 'in_progress');
      assert.equal(writes[0].conclusion, undefined);
      assert.match(writes[0].output.summary, new RegExp(': ' + status + '$', 'm'));
    }
  }
  const check = await existing(run(1, { status: 'in_progress', conclusion: null }));
  const { writes } = await execute({ checks: [check], slice: [], runs: [run(1)], engineRuns: [run(2)] });
  assert.equal(writes[0].status, 'completed');
  assert.equal(writes[0].conclusion, 'success');
});
test('a missing run of one workflow reports no fork CI run and links the other run', async () => {
  const { writes } = await execute({ runs: [run(1, { conclusion: 'failure' })], engineRuns: [] });
  assert.equal(writes[0].conclusion, 'neutral');
  assert.equal(writes[0].output.title, 'No fork CI run for this commit');
  assert.match(writes[0].output.summary, /^No Java CI Engines run was found for /);
  assert.ok(writes[0].output.summary.includes('- [Java CI](' + runUrl(1) + '): failure'));
  assert.match(writes[0].output.summary, /only docs or images is skipped by the path filter/);
  assert.equal(writes[0].details_url, runUrl(1));
  assert.deepEqual(JSON.parse(writes[0].output.text).runs.map((item) => item.run_id), [1, null]);
});
test('a completed check missing one run keeps being polled outside the sweep slice', async () => {
  const missing = await execute({ engineRuns: [] });
  const check = { ...await existing(), ...missing.writes[0] };
  const { writes } = await execute({ checks: [check], slice: [], engineRuns: [run(2)] });
  assert.equal(writes[0].conclusion, 'success');
});
test('one disabled or missing workflow asks the contributor to enable CI', async () => {
  for (const options of [
    { engineRuns: [], engineWorkflowState: 'disabled_manually' },
    { engineRuns: [], engineWorkflowState: null },
    { runs: [], engineRuns: [run(2)], workflowState: 'disabled_fork', engineWorkflowState: 'active' },
  ]) {
    const { writes } = await execute(options);
    assert.equal(writes[0].conclusion, 'action_required', JSON.stringify(options));
    assert.equal(writes[0].output.title, 'Enable CI on your fork');
    assert.equal(writes[0].details_url, 'https://github.com/contributor/hudi/actions');
  }
});
test('a rerun of one workflow updates the combined check', async () => {
  const check = await existing();
  const { writes } = await execute({
    checks: [check], engineRuns: [run(1, { run_attempt: 2, conclusion: 'failure' })],
  });
  assert.equal(writes[0].method, 'update');
  assert.equal(writes[0].conclusion, 'failure');
  assert.deepEqual(JSON.parse(writes[0].output.text).runs.map((item) => item.run_attempt), [1, 2]);
});


// Evaluates the bot.yml and java_ci_engines.yml expressions as JavaScript, which shares their
// syntax for the operators used there. needs.*.result becomes the list of the job results.
function evaluate(expression, scope) {
  const body = expression.replace(/needs\.\*\.result/g, 'Object.values(needs).map((job) => job.result)');
  return new Function(...Object.keys(scope), 'return (' + body + ');')(...Object.values(scope));
}
function interpolate(value, scope) {
  const whole = /^\s*\$\{\{((?:(?!\}\})[\s\S])*)\}\}\s*$/.exec(value);
  if (whole) return evaluate(whole[1], scope);
  return value.replace(/\$\{\{((?:(?!\}\})[\s\S])*)\}\}/g, (_, expression) => evaluate(expression, scope));
}
const functions = {
  always: () => true,
  startsWith: (text, prefix) => text.startsWith(prefix),
  contains: (list, item) => list.includes(item),
};
const withName = (ci, github) => ({ ...github, workflow: ci.name });
const group = (ci, github) => interpolate(ci.workflow.concurrency.group, { github: withName(ci, github) });
const cancels = (ci, github) => interpolate(ci.workflow.concurrency['cancel-in-progress'], { github: withName(ci, github) });
const jobIf = (ci, job, github) => interpolate(ci.workflow.jobs[job].if, { github: withName(ci, github), ...functions });
const testJobs = (ci) => Object.keys(ci.workflow.jobs).filter((job) => job !== ci.aggregate);
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
// PyYAML resolves the `*route` aliases, so every test job shows the expanded condition.
test('every test job carries the route condition, and the aggregate job does not', () => {
  for (const ci of javaCi) {
    const jobs = testJobs(ci);
    assert.ok(jobs.length > 1, ci.file);
    for (const job of jobs) {
      assert.equal(ci.workflow.jobs[job].if, '${{ !(' + FORK_PR + ') && !(' + UPSTREAM_OTHER_PUSH + ') }}', ci.file + ' ' + job);
    }
    const aggregate = ci.workflow.jobs[ci.aggregate];
    assert.equal(aggregate.if, '${{ always() && !(' + UPSTREAM_OTHER_PUSH + ') }}', ci.file);
    assert.equal(aggregate.env.FORK_PR, '${{ ' + FORK_PR + ' }}', ci.file);
    // A test job missing from needs would never fail the aggregate.
    assert.deepEqual([...aggregate.needs].sort(), [...jobs].sort(), ci.file);
  }
});

// [case, context, whether the test jobs run, whether the aggregate job runs]
for (const [name, github, tests, aggregate] of [
  ['fork PR to master', forkPr, false, true],
  ['fork PR to release-1.2', { ...forkPr, base_ref: 'release-1.2' }, true, true],
  ['fork PR to branch-0.x', { ...forkPr, base_ref: 'branch-0.x' }, true, true],
  ['same-repository PR to master', upstreamPr, true, true],
  ['merge queue', queue, true, true],
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
    for (const ci of javaCi) {
      for (const job of testJobs(ci)) assert.equal(jobIf(ci, job, github), tests, ci.file + ' ' + job);
      assert.equal(jobIf(ci, ci.aggregate, github), aggregate, ci.file + ' ' + ci.aggregate);
    }
  });
}
function aggregateFails(ci, github, results) {
  const job = ci.workflow.jobs[ci.aggregate];
  const env = { FORK_PR: String(interpolate(job.env.FORK_PR, { github })) };
  const step = job.steps.find((item) => item.name === 'Fail unless every job succeeded');
  const needs = Object.fromEntries(results.map((result, index) => ['job-' + index, { result }]));
  return interpolate(step.if, { github, env, needs, ...functions });
}
test('the aggregate jobs accept skipped test jobs only on fork PRs to master', () => {
  for (const ci of javaCi) {
    assert.equal(aggregateFails(ci, forkPr, ['skipped', 'skipped', 'skipped']), false, ci.file);
    for (const results of [['skipped', 'failure'], ['cancelled', 'skipped'], ['success', 'skipped'], ['success', 'success']]) {
      assert.equal(aggregateFails(ci, forkPr, results), true, ci.file + ' ' + results.join());
    }
    for (const github of [upstreamPr, queue, forkPush, upstreamPush('master'), { ...forkPr, base_ref: 'release-1.2' }]) {
      assert.equal(aggregateFails(ci, github, ['success', 'success', 'success']), false, ci.file);
      for (const results of [['skipped', 'skipped'], ['success', 'skipped'], ['failure', 'success'], ['success', 'cancelled']]) {
        assert.equal(aggregateFails(ci, github, results), true, ci.file + ' ' + results.join());
      }
    }
  }
});
test('each aggregate job links the fork run of its own workflow on fork PRs', () => {
  for (const ci of javaCi) {
    const step = ci.workflow.jobs[ci.aggregate].steps.find((item) => item.name === 'Link to fork CI');
    assert.equal(evaluate(step.if, { env: { FORK_PR: 'true' } }), true);
    assert.equal(evaluate(step.if, { env: { FORK_PR: 'false' } }), false);
    assert.ok(step.run.includes('/actions/workflows/' + ci.file + ')'), ci.file);
    assert.doesNotMatch(step.run, /\$\{\{/, ci.file);
  }
});

test('successive fork pushes share a cancellation group, separate branches do not', () => {
  for (const ci of javaCi) {
    assert.equal(cancels(ci, forkPush), true);
    assert.equal(group(ci, forkPush), group(ci, { ...forkPush, sha: 'second' }));
    assert.notEqual(group(ci, forkPush), group(ci, { ...forkPush, ref: 'refs/heads/other' }));
  }
});
test('the two workflows never share a concurrency group', () => {
  for (const github of [forkPush, forkPr, queue, upstreamPush('master')]) {
    assert.notEqual(group(javaCi[0], github), group(javaCi[1], github));
  }
});
// Unlike arrow-rs, event_name keeps manual runs apart from push runs, on forks as upstream.
test('fork manual runs neither replace nor cancel push runs', () => {
  for (const ci of javaCi) {
    const manual = { ...forkPush, event_name: 'workflow_dispatch' };
    assert.equal(cancels(ci, manual), false);
    assert.notEqual(group(ci, forkPush), group(ci, manual));
    assert.notEqual(group(ci, manual), group(ci, { ...manual, ref: 'refs/heads/other' }));
    assert.notEqual(group(ci, manual), group(ci, forkPr));
  }
});
test('upstream pushes and merge queue runs remain separate per commit and never cancel', () => {
  for (const ci of javaCi) {
    const push = upstreamPush('master');
    assert.equal(cancels(ci, push), false);
    assert.notEqual(group(ci, push), group(ci, { ...push, sha: 'second' }));
    // Each queue entry has a ref of its own, which names the PR and the commit.
    assert.equal(cancels(ci, queue), false);
    assert.notEqual(group(ci, queue), group(ci, {
      ...queue, ref: 'refs/heads/gh-readonly-queue/master/pr-2-second', sha: 'second',
    }));
    for (const event_name of ['workflow_dispatch', 'merge_group']) {
      assert.notEqual(group(ci, push), group(ci, { ...push, event_name }));
    }
  }
});
test('same-named branches from different fork PRs cannot cancel each other', () => {
  for (const ci of javaCi) {
    assert.equal(cancels(ci, forkPr), true);
    assert.notEqual(group(ci, forkPr), group(ci, { ...forkPr, event: { pull_request: { number: 2 } } }));
  }
});
