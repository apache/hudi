---
sidebar_position: 4
title: "RFC Process"
toc: true
last_modified_at: 2020-09-01T15:59:57-04:00
---

This page describes a Request For Comments (RFC) process for proposing any major change to Hudi or even just sharing designs/vision for the project to get early feedback.

## When do I need an RFC ?

Whenever a feature is considered to be causing a “major change” in the project, such a feature requires an RFC. 
Any of the following can be considered a major change:

 - Any new component, module or code that introduces a new concept into the project or alters the behavior of an existing one
 - Any large code refactor to address general code re-usability and structure. There is no strong definition for “Large” and whether or not the refactor requires an RFC can be discussed on the @dev mailing list.
 - Any change that impacts the underlying storage layout/format. e.g changes to the HoodieLogFormat, timeline layout version.
 - New indexing schemes, New deltastreamer sources, New platform services.
 - Any change that impacts the public interfaces of the project.

It can also be used to describe large direction shifts to the project (e.g. Flink support) or new green field ideas (e.g. Hudi for ML flows)
Not all RFCs require the same effort and detail. For critical changes such as the File Format and Index, we need to deeply discuss the trade-offs of 
making such a change and how it impacts current and new users. Any changes to such components affect the correctness of a dataset (backwards and forward versions supported). 
Other changes such as code refactor might require more details around abstractions but as long as there is good test coverage, a migration plan section can be avoided. 
It may happen that you are making a bunch of changes across many components to enable an already existing feature. For example, introducing a new config along with reporting metrics, 
enhancing a tool and also improving documentation and the on-boarding experience. If all of these changes are linked to a general feature/idea, these can be grouped together under a single RFC.

## Who can initiate the RFC ?
Anyone can initiate an RFC. Please note that if you are unsure of whether a feature already exists or if there is a plan already to implement a similar one, always start a discussion thread on the dev mailing list before initiating a RFC. This will help everyone get the right context and optimize everyone’s usage of time.

## How do I author an RFC ?

### Proposing the RFC
1. First, start a discussion thread on the Apache Hudi dev mailing list, by sending an email to `dev@hudi.apache.org` with subject line `DISCUSS <proposed idea>`. 
Use this discussion thread to get an agreement from people on the mailing list that your proposed idea necessitates an RFC.
2. Raise a PR, adding an entry to the table at `rfc/README.md`, picking the next available RFC number. Hudi committers will help land that.

### Writing the RFC
1. Create a folder `rfc-<number>` under `rfc` folder, where `<number>` is replaced by the actual RFC number used.
2. Copy the rfc template file `rfc/template.md` to `rfc/rfc-<number>/rfc-<number>.md` and proceed to draft your design document.
3. [Optional] Place any images used by the same directory using the `![alt text](./image.png)` markdown syntax.
4. Add at least 2 PMC members as approvers (you can find their github usernames [here](/community/team)). You are free to add any number of dev members to your reviewers list.
5. Raise a PR against the master branch with `[RFC-<number>]` in the title and work through feedback, until the RFC approved (by approving the Github PR itself)
6. Before landing the PR, please change the status to "IN PROGRESS" under `rfc/README.md` and keep it maintained as you go about implementing, completing or even abandoning.

### Keeping it upto date

RFC process is by no-means is an attempt at the "waterfall" software development process. 
We recognize that design can be an iterative process as well, often evolving together with implementation. 

1. Please ensure your code PRs are labelled with a `[RFC-<number>]` title.
2. As your implementation changes over time, you will update the RFCs and keep them in-sync. Project maintainers could point out these opportunities during code reviews.
3. During the final landing of all goals of the RFC, the status changes to "COMPLETED"


