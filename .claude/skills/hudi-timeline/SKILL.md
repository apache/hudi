---
name: hudi-timeline
description: Explain or debug the Hudi timeline. Use when asking about instants, actions, states, stuck operations, archival, or timeline corruption.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [question e.g. "what is the timeline", "stuck inflight instant", "timeline too large", "archival not working"]
---

# Hudi Timeline

Question: **$ARGUMENTS**

## Instructions

### Key source files:
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieTimeline.java` - Core timeline interface
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieActiveTimeline.java` - Active timeline interface
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieArchivedTimeline.java` - Archived timeline interface
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieInstant.java` - Single timeline instant
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/timeline/HoodieTimelineArchiver.java` - Archival logic

### Timeline fundamentals:
- The timeline lives in the `.hoodie/` directory of each table
- Each instant has: **action** (commit, compaction, clean, etc.), **state** (REQUESTED, INFLIGHT, COMPLETED), **timestamp**
- State machine: REQUESTED -> INFLIGHT -> COMPLETED (or REQUESTED -> INFLIGHT -> rolled back)
- Active timeline: recent instants in `.hoodie/` directory
- Archived timeline: old instants moved to `.hoodie/archived/` (LSM-based storage)

### Action types (from HoodieTimeline):
- `commit` - CoW write
- `deltacommit` - MoR write
- `compaction` - MoR compaction
- `logcompaction` - MoR log compaction
- `clean` - File cleanup
- `rollback` - Undo a failed action
- `savepoint` - Pinned snapshot
- `replacecommit` - Clustering or insert_overwrite
- `restore` - Restore to savepoint
- `indexing` - Index building

### Common timeline issues:

**Stuck INFLIGHT instant:**
- Writer crashed before completing
- Check heartbeat files: `.hoodie/.heartbeat/<instant_time>`
- If heartbeat expired, safe to rollback
- Rollback: `CALL rollback_to_instant_time(table => '<name>', instant_time => '<time>');`

**Timeline too large (.hoodie directory growing):**
- Archival not keeping up
- Configs: `hoodie.keep.min.commits` (default: 20), `hoodie.keep.max.commits` (default: 30)
- Manual trigger: `CALL archive_commits(table => '<name>');`

**Timeline corruption:**
- Missing instant files
- Inconsistent state (COMPLETED without INFLIGHT)
- Use repair tools: `HoodieRepairTool` in `hudi-utilities`

### Diagnostic commands:
```sql
CALL show_timeline(path => '<path>', limit => 50);
CALL show_commits(path => '<path>', limit => 20);
CALL show_archived_commits(path => '<path>', limit => 20);
CALL show_rollbacks(path => '<path>', limit => 10);
```
