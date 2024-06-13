#!/usr/bin/env bash

# usage is ./release <commit-sha-for-previous-release> (you can get it from CHANGELOG - 1st commit sha under the previous release)

echo "" >> CHANGELOG
echo "---[$(date '+%Y-%m-%d %H:%M:%S')] [$(git branch --show-current)] ---" >> CHANGELOG
git log $1..HEAD --pretty=format:"%h %s" >> CHANGELOG
echo "" >> CHANGELOG