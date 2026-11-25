# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Check the secured Service using its host:port and the Spark client token environment."""
import argparse
import os

import grpc
from pyspark.errors.exceptions.connect import SparkConnectGrpcException
from pyspark.sql import SparkSession


def query(uri, *, retry=True):
    spark = SparkSession.builder.remote(uri).create()
    try:
        if not retry:
            spark.client.set_retry_policies([])
        rows = spark.sql('SELECT 1').collect()
        if rows != [(1,)]:
            raise AssertionError(f'Unexpected SELECT result: {rows!r}')
    finally:
        spark.stop()


def main(target, expect_untrusted_ca):
    uri = f'sc://{target}/;use_ssl=true'
    token_env = 'SPARK_CONNECT_AUTHENTICATE_TOKEN'
    if not os.environ.get(token_env):
        raise ValueError(f'The authenticated query requires {token_env}')
    if expect_untrusted_ca:
        try:
            query(uri, retry=False)
        except SparkConnectGrpcException as exc:
            if (exc.getGrpcStatusCode() != grpc.StatusCode.UNAVAILABLE
                    or 'CERTIFICATE_VERIFY_FAILED' not in str(exc)):
                raise
            print('PASS: PySpark rejected the untrusted certificate', flush=True)
            return
        raise AssertionError('PySpark accepted the untrusted certificate')

    with grpc.insecure_channel(target) as channel:
        ready = grpc.channel_ready_future(channel)
        try:
            ready.result(timeout=5)
        except grpc.FutureTimeoutError:
            ready.cancel()
        else:
            raise AssertionError('Service accepted a plaintext gRPC connection')

    query(uri)
    print('PASS: verified TLS and authenticated SELECT; plaintext rejected', flush=True)

    del os.environ[token_env]
    for case in ('missing', 'incorrect'):
        if case == 'incorrect':
            os.environ[token_env] = 'incorrect-ci-token'
        try:
            query(uri)
        except SparkConnectGrpcException as exc:
            if exc.getGrpcStatusCode() != grpc.StatusCode.UNAUTHENTICATED:
                raise
        else:
            raise AssertionError(f'Service accepted {case} token')
        print(f'PASS: {case} token rejected as UNAUTHENTICATED', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('target', metavar='host:port')
    parser.add_argument('--expect-untrusted-ca', action='store_true',
                        help='require the authenticated PySpark query to fail certificate verification')
    args = parser.parse_args()
    main(args.target, args.expect_untrusted_ca)
