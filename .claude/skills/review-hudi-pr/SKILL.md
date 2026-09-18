---
name: review-hudi-pr
description: Review a Hudi PR per CLAUDE.md standards. Use when reviewing pull requests for correctness, design, and breaking changes.
user-invocable: true
disable-model-invocation: true
allowed-tools: Read, Grep, Glob, Bash, Agent
argument-hint: [PR number or URL e.g. "18133" or "https://github.com/apache/hudi/pull/18133"]
---

# Review Hudi Pull Request

PR: **$ARGUMENTS**

## Instructions

Follow the code review standards defined in CLAUDE.md at the root of this repository. The full review process is defined there.

### Step 1: Gather context
```bash
# Get PR description and metadata
gh pr view $ARGUMENTS --json title,body,labels,files,additions,deletions
# Get PR diff
gh pr diff $ARGUMENTS
# Get existing comments (avoid duplicating)
gh api repos/apache/hudi/pulls/$ARGUMENTS/comments
```

### Step 2: Analyze the change
Read the actual changed files in the local repo to understand context around the diff.

Apply the review checklist from CLAUDE.md:
- **Breaking changes**: Check @PublicAPIClass/@PublicAPIMethod, config renames, storage format changes
- **Concurrency & data correctness**: Timeline operations, lock handling, atomic operations
- **Resource leaks**: Unclosed streams, missing try-with-resources
- **Software design**: Layer violations, coupling, config quality
- **Testing**: Coverage, edge cases, test quality
- **Silent failures**: Empty catch blocks, swallowed exceptions

### Step 3: Apply the Net-Positive Principle
> "Does this change close doors, do breaking changes, make follow-on fixes cumbersome, or leave the codebase worse than before?"

If no -> approvable. Imperfections go in non-blocking comments.

### Step 4: Output format (MUST FOLLOW)
1. **Summary** (3-7 bullets) on overall mergeability
2. Question large PRs (>2000 lines) for breakdown potential
3. Note any breaking changes
4. Comments with severity levels:
   - BLOCKING - Must fix before merge
   - IMPORTANT - Should fix but PR still net-positive
   - SUGGESTION - Take or leave
   - NIT - Style/preference only
