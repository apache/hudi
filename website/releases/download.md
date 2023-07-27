---
title: Download
sidebar_position: 1
keywords: [ hudi, download]
toc: true
last_modified_at: 2022-12-27T15:59:57-04:00
---

### Release 0.13.1
* Source Release : [Apache Hudi 0.13.1 Source Release](https://www.apache.org/dyn/closer.lua/hudi/0.13.1/hudi-0.13.1.src.tgz) ([asc](https://downloads.apache.org/hudi/0.13.1/hudi-0.13.1.src.tgz.asc), [sha512](https://downloads.apache.org/hudi/0.13.1/hudi-0.13.1.src.tgz.sha512))
* Release Note : ([Release Note for Apache Hudi 0.13.1](/releases/release-0.13.1))

### Release 0.12.3
[Long Term Support](/releases/release-0.12.3#long-term-support): this is the latest stable release
* Source Release : [Apache Hudi 0.12.3 Source Release](https://www.apache.org/dyn/closer.lua/hudi/0.12.3/hudi-0.12.3.src.tgz) ([asc](https://downloads.apache.org/hudi/0.12.3/hudi-0.12.3.src.tgz.asc), [sha512](https://downloads.apache.org/hudi/0.12.3/hudi-0.12.3.src.tgz.sha512))
* Release Note : ([Release Note for Apache Hudi 0.12.3](/releases/release-0.12.3))

### Release 0.11.1
* Source Release : [Apache Hudi 0.11.1 Source Release](https://www.apache.org/dyn/closer.lua/hudi/0.11.1/hudi-0.11.1.src.tgz) ([asc](https://downloads.apache.org/hudi/0.11.1/hudi-0.11.1.src.tgz.asc), [sha512](https://downloads.apache.org/hudi/0.11.1/hudi-0.11.1.src.tgz.sha512))
* Release Note : ([Release Note for Apache Hudi 0.11.1](/releases/release-0.11.1))

### Older releases
As new Hudi releases come out for each development stream, previous ones will be archived, but they are still available at [here](https://archive.apache.org/dist/hudi/). 

## Verify Release

It is essential that you verify the integrity of the downloaded files using the PGP signatures. Please read [How to Verify Downloaded Files](https://www.apache.org/info/verification.html)
for more information on how and why you should verify our releases.

The PGP signatures can be verified using PGP or GPG. First download the [KEYS](https://downloads.apache.org/hudi/KEYS) file as well as the
.asc signature files for the relevant release packages. Make sure you get these files from the main distribution directory, rather than from
a mirror. Then verify the signatures using:

```
% pgpk -a KEYS
% pgpv hudi-X.Y.Z.src.tgz.asc
```

or

```
% pgp -ka KEYS
% pgp hudi-X.Y.Z.src.tgz.asc
```

or

```
% gpg --import KEYS
% gpg --verify hudi-X.Y.Z.src.tgz.asc hudi-X.Y.Z.src.tgz
```