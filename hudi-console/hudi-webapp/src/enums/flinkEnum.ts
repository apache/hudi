/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export enum BuildStateEnum {
  /** has changed, need rebuild */
  NEED_REBUILD = -2,
  /** has cancelled, not build */
  NOT_BUDIL = -1,
  /** building */
  BUILDING = 0,
  /** build successful */
  SUCCESSFUL = 1,
  /** build failed  */
  FAILED = 2,
}
/* ExecutionMode  */
export enum ExecModeEnum {
  /** remote (standalone) */
  REMOTE = 1,
  /** yarn per-job (deprecated, please use yarn-application mode) */
  YARN_PER_JOB = 2,
  /** yarn session */
  YARN_SESSION = 3,
  /** yarn application */
  YARN_APPLICATION = 4,
  /** kubernetes session */
  KUBERNETES_SESSION = 5,
  /** kubernetes application */
  KUBERNETES_APPLICATION = 6,
}

export enum ReleaseStateEnum {
  /** release failed */
  FAILED = -1,
  /** release done */
  DONE = 0,
  /** need release after modify task */
  NEED_RELEASE = 1,
  /** releasing */
  RELEASING = 2,
  /** release complete, need restart */
  NEED_RESTART = 3,
  /** need rollback */
  NEED_ROLLBACK = 4,
  /** project has changed, need to check the jar whether to be re-selected */
  NEED_CHECK = 5,
  /** revoked  */
  REVOKED = 10,
}

export enum OptionStateEnum {
  /** Application which is currently action: none. */
  NONE = 0,
  /** Application which is currently action: deploying. */
  RELEASING = 1,
  /** Application which is currently action: cancelling. */
  CANCELLING = 2,
  /** Application which is currently action: starting. */
  STARTING = 3,
  /** Application which is currently action: savepointing. */
  SAVEPOINTING = 4,
}

export enum AppStateEnum {
  /** added new job to database  */
  ADDED = 0,
  /** The job has been received by the Dispatcher, and is waiting for the job manager to be created. */
  INITIALIZING = 1,
  /** Job is newly created, no task has started to run. */
  CREATED = 2,
  /** Application which is currently starting. */
  STARTING = 3,
  /** Application which is currently running. */
  RESTARTING = 4,
  /** Some tasks are scheduled or running, some may be pending, some may be finished. */
  RUNNING = 5,
  /** The job has failed and is currently waiting for the cleanup to complete. */
  FAILING = 6,
  /** The job has failed with a non-recoverable task failure.*/
  FAILED = 7,
  /** Job is being cancelled. */
  CANCELLING = 8,
  /** Job has been cancelled. */
  CANCELED = 9,
  /** All the job's tasks have successfully finished. */
  FINISHED = 10,
  /** The job has been suspended which means that it has been stopped but not been removed from a potential HA job store. */
  SUSPENDED = 11,
  /** The job is currently reconciling and waits for task execution report to recover state. */
  RECONCILING = 12,
  /** Lost */
  LOST = 13,
  /** MAPPING */
  MAPPING = 14,
  /** OTHER */
  OTHER = 15,
  /** has rollback */
  REVOKED = 16,
  /**
   * Lost track of flink job temporarily.
   * A complete loss of flink job tracking translates into LOST state.
   */
  SILENT = 17,
  /** Flink job has terminated vaguely, maybe FINISHED, CANCELED or FAILED */
  TERMINATED = 18,
  /** Flink job has terminated vaguely, maybe FINISHED, CANCELED or FAILED */
  POS_TERMINATED = 19,
  /** job SUCCEEDED on yarn */
  SUCCEEDED = 20,
  /** has killed in Yarn */
  KILLED = -9,
}

export enum ClusterStateEnum {
  /** The cluster was just created but not started */
  CREATED = 0,
  /** cluster started */
  STARTED = 1,
  /** cluster canceled */
  CANCELED = 2,
  /** cluster lost */
  LOST = 3,
}

export enum AppTypeEnum {
  /** StreamPark Flink */
  STREAMPARK_FLINK = 1,
  /** Apache Flink */
  APACHE_FLINK = 2,
  /** StreamPark Spark */
  STREAMPARK_SPARK = 3,
  /** Apache Spark */
  APACHE_SPARK = 4,
}

export enum JobTypeEnum {
  JAR = 1,
  SQL = 2,
}

export enum ConfigTypeEnum {
  /** yaml type */
  YAML = 1,
  /** properties type */
  PROPERTIES = 2,
  /** HOCON config type */
  HOCON = 3,
  /** unknown */
  UNKNOWN = 0,
}

export enum CandidateTypeEnum {
  /** non candidate */
  NONE = 0,
  /** newly added record becomes a candidate */
  NEW = 1,
  /** specific history becomes a candidate */
  HISTORY = 2,
}

export enum ResourceFromEnum {
  /** cicd(build from cvs) */
  CICD = 1,
  /** upload local jar */
  UPLOAD = 2,
}

export enum UseStrategyEnum {
  /** use existing */
  USE_EXIST = 1,
  /** reselect */
  RESELECT = 2,
}

export enum SavePointEnum {
  CHECK_POINT = 1,
  SAVE_POINT = 2,
}

export enum PipelineStepEnum {
  UNKNOWN = 0,
  WAITING = 1,
  RUNNING = 2,
  SUCCESS = 3,
  FAILURE = 4,
  SKIPPED = 5,
}

export enum AlertTypeEnum {
  /** mail */
  MAIL = 1,
  /** dingtalk */
  DINGTALK = 2,
  /** wecom */
  WECOM = 4,
  /**message */
  MESSAGE = 8,
  /** lark */
  LARK = 16,
}

export enum FailoverStrategyEnum {
  ALERT = 1,
  RESTART = 2,
}
