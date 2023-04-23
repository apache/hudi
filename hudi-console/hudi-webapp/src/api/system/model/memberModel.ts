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
export interface MemberListRecord {
  id: string;
  userId: string;
  roleId: string;
  createTime: string;
  modifyTime: string;
  userName: string;
  roleName: string;
  sortField?: string;
  sortOrder?: string;
  createTimeFrom?: string;
  createTimeTo?: string;
}
export interface AddMemberParams {
  userName: string;
  roleId: number;
}

export interface UpdateMemberParams extends AddMemberParams {
  id: string;
  userId: string;
}
