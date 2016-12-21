#!/usr/bin/env bash

set -e

SCRIPT_DIR=`dirname $0`
DEPLOY_DIR=$SCRIPT_DIR/..
openssl aes-256-cbc -pass pass:$gpg.passphrase -in $DEPLOY_DIR/pubring.gpg.enc -out pubring.gpg -d
openssl aes-256-cbc -pass pass:$gpg.passphrase -in $DEPLOY_DIR/secring.gpg.enc -out secring.gpg -d
