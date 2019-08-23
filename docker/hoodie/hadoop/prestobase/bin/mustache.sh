#!/bin/sh

# `mustache.sh`, Mustache in POSIX shell.

set -e

# Load the `mustache` function and its friends.  These are assumed to be
# in the `lib` directory in the same tree as this `bin` directory.
. "$(dirname "$(dirname "$0")")/lib/mustache.sh"

# Call `mustache` to make this behave somewhat like `mustache`(1).
# Because it doesn't accept the `--compile` or `--tokens` command-line
# options and does not accept input file(s) as arguments, this program
# is called `mustache.sh`(1), not `mustache`(1).
mustache