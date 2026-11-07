#!/usr/bin/env python3
"""Re-stamps restored build outputs so a later `mvn test` sees them as up to date.

The archive keeps the modification times of the build job, which are older than the
sources of the fresh checkout in this job, so the compilers would treat every module as
stale. Touching everything with one time is not enough either, because the order between
outputs matters: maven-jar-plugin rebuilds a test-jar whose classes are not older than
the jar (and maven-compiler-plugin then recompiles every module depending on it), and
scala-maven-plugin recompiles when a source (e.g. ANTLR output under target/) is not
older than its timestamp file, in milliseconds.

So every distinct original millisecond gets its own millisecond, in the original order,
starting just after the newest checked-out source file. Outputs that had the same
millisecond keep it, outputs that differed stay apart, all of them are newer than the
sources, and the last one is at least a second older than the next Maven invocation.

Usage: run from the workspace root: compress_mtimes.py <root> [<root> ...]
"""
import os
import sys
import time

MS = 1_000_000


def walk(roots):
    for root in roots:
        yield root
        for dirpath, dirnames, filenames in os.walk(root):
            for name in dirnames + filenames:
                yield os.path.join(dirpath, name)


def newest_source_ns(workspace, roots):
    skip = {os.path.realpath(r) for r in roots}
    newest = 0
    for dirpath, dirnames, filenames in os.walk(workspace):
        dirnames[:] = [d for d in dirnames if d not in (".git", "target")
                       and os.path.realpath(os.path.join(dirpath, d)) not in skip]
        for name in filenames:
            newest = max(newest, os.lstat(os.path.join(dirpath, name)).st_mtime_ns)
    return newest


def refresh_checkstyle_caches(roots):
    """maven-checkstyle-plugin caches results per source path and modification time. The
    archive is from this very commit (the manifest is checked), so the cached results still
    hold for the checked-out sources; point them at the new modification times."""
    for root in roots:
        cache = os.path.join(root, "checkstyle-cachefile")
        if not os.path.isfile(cache):
            continue
        out = []
        with open(cache) as f:
            for line in f:
                key, sep, _ = line.rstrip("\n").partition("=")
                if sep and os.path.isabs(key) and os.path.isfile(key):
                    line = f"{key}={os.stat(key).st_mtime_ns // MS}\n"
                out.append(line)
        with open(cache, "w") as f:
            f.writelines(out)


def main(roots):
    refresh_checkstyle_caches(roots)
    entries = [(p, os.lstat(p).st_mtime_ns // MS, os.path.islink(p)) for p in walk(roots)]
    if not entries:
        return
    ranks = {ms: i for i, ms in enumerate(sorted({e[1] for e in entries}))}
    start = (newest_source_ns(".", roots) // MS + 1000) * MS
    end = start + len(ranks) * MS
    wait = (end + 1000 * MS - time.time_ns()) / 1e9
    if wait > 0:
        time.sleep(wait)
    # Deepest paths first, so stamping a file never bumps an already stamped directory.
    entries.sort(key=lambda e: e[0].count(os.sep), reverse=True)
    for path, ms, is_link in entries:
        stamp = start + ranks[ms] * MS
        os.utime(path, ns=(stamp, stamp), follow_symlinks=not is_link)
    print(f"re-stamped {len(entries)} paths onto {len(ranks)} distinct milliseconds"
          f" after the newest source (waited {max(wait, 0):.1f}s)")


if __name__ == "__main__":
    main(sys.argv[1:])
