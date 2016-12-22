/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.model;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Manages the commit meta and provides operations on the commit timeline
 */
public class HoodieCommits implements Serializable {

    private List<String> commitList;

    public HoodieCommits(List<String> commitList) {
        this.commitList = new ArrayList<>(commitList);
        Collections.sort(this.commitList);
        this.commitList = Collections.unmodifiableList(this.commitList);
    }

    /**
     * Returns the commits which are in the range (startsTs, endTs].
     *
     * @param startTs - exclusive start commit ts
     * @param endTs   - inclusive end commit ts
     */
    public List<String> findCommitsInRange(String startTs, String endTs) {
        if (commitList.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        int startIndex = 0;
        if (startTs != null) {
            startIndex = Collections.binarySearch(commitList, startTs);
            // If startIndex is negative
            if (startIndex < 0) {
                startIndex = -(startIndex + 1);
            }
        }

        int endIndex = Collections.binarySearch(commitList, endTs);
        // If endIndex is negative
        if (endIndex < 0) {
            endIndex = -(endIndex + 1);
        }

        if (endIndex < startIndex) {
            throw new IllegalArgumentException(
                    "Start Commit Ts " + startTs + " cannot be less than end commit ts" + endTs);
        }
        List<String> returns = new ArrayList<>(commitList.subList(startIndex, endIndex));
        if(endIndex < commitList.size()) {
            // Be inclusive of the endIndex
            returns.add(commitList.get(endIndex));
        }
        return Collections.unmodifiableList(returns);
    }

    /**
     * Finds the list of commits on or before asOfTs
     */
    public List<String> findCommitsAfter(String commitTimeStamp, int numCommits) {
        if (commitList.isEmpty()) {
            return null;
        }

        int startIndex = Collections.binarySearch(commitList, commitTimeStamp);
        if (startIndex < 0) {
            startIndex = -(startIndex + 1);
        } else {
            // we found asOfTs at startIndex. We want to exclude it.
            startIndex++;
        }


        List<String> commits = new ArrayList<>();
        while (numCommits > 0 && startIndex < commitList.size()) {
            commits.add(commitList.get(startIndex));
            startIndex++;
            numCommits--;
        }

        return Collections.unmodifiableList(commits);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HoodieCommits{");
        sb.append("commitList=").append(commitList);
        sb.append('}');
        return sb.toString();
    }

    public boolean isEmpty() {
        return commitList.isEmpty();
    }

    public int getNumCommits() {
        return commitList.size();
    }

    public String firstCommit() {
        return commitList.isEmpty() ? null : commitList.get(0);
    }

    public String nthCommit(int n) {
        return commitList.isEmpty() || n >= commitList.size() ? null : commitList.get(n);
    }

    public String lastCommit() {
        return commitList.isEmpty() ? null : commitList.get(commitList.size() - 1);
    }

    /**
     * Returns the nth commit from the latest commit such that lastCommit(0) gteq lastCommit()
     */
    public String lastCommit(int n) {
        if (commitList.size() < n + 1) {
            return null;
        }
        return commitList.get(commitList.size() - 1 - n);
    }

    public boolean contains(String commitTs) {
        return commitList.contains(commitTs);
    }

    public String max(String commit1, String commit2) {
        if (commit1 == null && commit2 == null) {
            return null;
        }
        if (commit1 == null) {
            return commit2;
        }
        if (commit2 == null) {
            return commit1;
        }
        return (isCommit1BeforeOrOn(commit1, commit2) ? commit2 : commit1);
    }

    public static boolean isCommit1BeforeOrOn(String commit1, String commit2) {
        return commit1.compareTo(commit2) <= 0;
    }

    public static boolean isCommit1After(String commit1, String commit2) {
        return commit1.compareTo(commit2) > 0;
    }

    public List<String> getCommitList() {
        return commitList;
    }

    public boolean isCommitBeforeEarliestCommit(String commitTs) {
        return isCommit1BeforeOrOn(commitTs, firstCommit());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        HoodieCommits that = (HoodieCommits) o;

        return commitList != null ? commitList.equals(that.commitList) : that.commitList == null;

    }

    @Override
    public int hashCode() {
        return commitList != null ? commitList.hashCode() : 0;
    }

}
