/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


-- ----------------------------
-- Records of t_team
-- ----------------------------
insert into "public"."t_team" values (100000, 'default', null, now(), now());


-- ----------------------------
-- Records of t_flink_app
-- ----------------------------
insert into "public"."t_flink_app" values (100000, 100000, 2, 4, null, null, 'Flink SQL Demo', null, null, null, null, null, null , null, 100000, null, 1, null, null, null, null, null, null, null, 0, 0, null, null, null, null, null, null, 'Flink SQL Demo', 0, null, null, null, null, null, null, null, 0, 0, now(), now(), null, 1, true, null, null, null, null, null, null, false, null, null, null, 'streampark,test');

-- ----------------------------
-- Records of t_flink_effective
-- ----------------------------
insert into "public"."t_flink_effective" values (100000, 100000, 2, 100000, now());

-- ----------------------------
-- Records of t_flink_project
-- ----------------------------
insert into "public"."t_flink_project" values (100000, 100000, 'streampark-quickstart', '1', 'https://github.com/apache/incubator-streampark-quickstart', 'release-2.0.0', null, null, null, null, null, 1, 1, null, 'streampark-quickstart', -1, now(), now());


-- ----------------------------
-- Records of t_flink_sql
-- ----------------------------
insert into "public"."t_flink_sql" values (100000, 100000, 'eNqlUUtPhDAQvu+vmFs1AYIHT5s94AaVqGxSSPZIKgxrY2mxrdGfb4GS3c0+LnJo6Mz36syapkmZQpk8vKbQMMt2KOFmAe5rK4Nf3yhrhCwvA1/TTDaqO61UxmooSprlT1PDGkgKEKpmwvIOjWVdP3W2zpG+JfQFHjfU46xxrVvYZuWztye1khJrqzSBFRCfjUwSYQiqt1xJJvyPcbWJp9WPCXvUoUEn0ZAVufcs0nIUjYn2L4s++YiY75eBLr+2Dnl3GYKTWRyfQKYRRR2XZxXmNvu9yh9GHAmUO/sxyMRkGNly4c714RZ7zaWtLHsX+N9NjvVrWxm99jmyvEhpOUhujmIYFI5zkCOYzYIj11a7QH7Tyz+nE8bw', null, 1, 1, now());

-- ----------------------------
-- Records of t_menu
-- ----------------------------
insert into "public"."t_menu" values (100000, 0, 'menu.system', '/system', 'PageView', null, 'desktop', '0', '1', 1, now(), now());
insert into "public"."t_menu" values (100001, 100000, 'menu.userManagement', '/system/user', 'system/user/User', null, 'user', '0', '1', 1, now(), now());
insert into "public"."t_menu" values (100002, 100000, 'menu.roleManagement', '/system/role', 'system/role/Role', null, 'smile', '0', '1', 2, now(), now());
insert into "public"."t_menu" values (100003, 100000, 'menu.menuManagement', '/system/menu', 'system/menu/Menu', 'menu:view', 'bars', '0', '1', 3, now(), now());
insert into "public"."t_menu" values (100004, 100001, 'add', null, null, 'user:add', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100005, 100001, 'update', null, null, 'user:update', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100006, 100001, 'delete', null, null, 'user:delete', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100007, 100002, 'add', null, null, 'role:add', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100008, 100002, 'update', null, null, 'role:update', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100009, 100002, 'delete', null, null, 'role:delete', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100012, 100001, 'reset', null, null, 'user:reset', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100013, 0, 'StreamPark', '/flink', 'PageView', null, 'build', '0', '1', 2, now(), now());
insert into "public"."t_menu" values (100014, 100013, 'menu.project', '/flink/project', 'flink/project/View', null, 'github', '0', '1', 1, now(), now());
insert into "public"."t_menu" values (100015, 100013, 'menu.application', '/flink/app', 'flink/app/View', null, 'mobile', '0', '1', 2, now(), now());
insert into "public"."t_menu" values (100016, 100015, 'add', '/flink/app/add', 'flink/app/Add', 'app:create', '', '0', '0', null, now(), now());
insert into "public"."t_menu" values (100017, 100014, 'add', '/flink/project/add', 'flink/project/Add', 'project:create', '', '0', '0', null, now(), now());
insert into "public"."t_menu" values (100018, 100015, 'detail app', '/flink/app/detail', 'flink/app/Detail', 'app:detail', '', '0', '0', null, now(), now());
-- insert into "public"."t_menu" values (100019, 100013, 'Notebook', '/flink/notebook/view', 'flink/notebook/Submit', 'notebook:submit', 'read', '0', '1', 4, now(), now());
insert into "public"."t_menu" values (100020, 100015, 'edit flink', '/flink/app/edit_flink', 'flink/app/EditFlink', 'app:update', '', '0', '0', null, now(), now());
insert into "public"."t_menu" values (100021, 100015, 'edit streampark', '/flink/app/edit_streampark', 'flink/app/EditStreamPark', 'app:update', '', '0', '0', null, now(), now());
insert into "public"."t_menu" values (100022, 100014, 'build', null, null, 'project:build', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100023, 100014, 'delete', null, null, 'project:delete', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100024, 100015, 'mapping', null, null, 'app:mapping', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100025, 100015, 'release', null, null, 'app:release', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100026, 100015, 'start', null, null, 'app:start', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100027, 100015, 'clean', null, null, 'app:clean', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100028, 100015, 'cancel', null, null, 'app:cancel', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100029, 100015, 'savepoint delete', null, null, 'savepoint:delete', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100030, 100015, 'backup rollback', null, null, 'backup:rollback', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100031, 100015, 'backup delete', null, null, 'backup:delete', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100032, 100015, 'conf delete', null, null, 'conf:delete', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100033, 100013, 'menu.setting', '/flink/setting', 'flink/setting/View', null, 'setting', '0', '1', 5, now(), now());
insert into "public"."t_menu" values (100034, 100033, 'setting update', null, null, 'setting:update', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100035, 100014, 'edit', '/flink/project/edit', 'flink/project/Edit', 'project:update', null, '0', '0', null, now(), now());
insert into "public"."t_menu" values (100036, 100015, 'delete', null, null, 'app:delete', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100037, 100000, 'menu.tokenManagement', '/system/token', 'system/token/Token', null, 'lock', '0', '1', 1, now(), now());
insert into "public"."t_menu" values (100038, 100037, 'add', null, null, 'token:add', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100039, 100037, 'delete', null, null, 'token:delete', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100040, 100033, 'add cluster', '/flink/setting/add_cluster', 'flink/setting/AddCluster', 'cluster:create', '', '0', '0', null, now(), now());
insert into "public"."t_menu" values (100041, 100033, 'edit cluster', '/flink/setting/edit_cluster', 'flink/setting/EditCluster', 'cluster:update', '', '0', '0', null, now(), now());
insert into "public"."t_menu" values (100042, 100015, 'copy', null, null, 'app:copy', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100043, 100000, 'menu.teamManagement', '/system/team', 'system/team/Team', null, 'team', '0', '1', 2, now(), now());
insert into "public"."t_menu" values (100044, 100043, 'add', null, null, 'team:add', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100045, 100043, 'update', null, null, 'team:update', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100046, 100043, 'delete', null, null, 'team:delete', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100047, 100000, 'menu.permissionsManagement', '/system/permissions', 'system/permissions/Permissions', null, 'usergroup-add', '0', '1', 2, now(), now());
insert into "public"."t_menu" values (100048, 100047, 'add', null, null, 'permissions:add', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100049, 100047, 'update', null, null, 'permissions:update', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100050, 100047, 'delete', null, null, 'permissions:delete', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100051, 100047, 'role view', null, null, 'role:view', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100052, 100001, 'types', null, null, 'user:types', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100053, 100013, 'menu.variable', '/flink/variable', 'flink/variable/View', null, 'code', '0', '1', 3, now(), now());
insert into "public"."t_menu" values (100054, 100053, 'add', NULL, NULL, 'variable:add', NULL, '1', '1', NULL, now(), now());
insert into "public"."t_menu" values (100055, 100053, 'update', NULL, NULL, 'variable:update', NULL, '1', '1', NULL, now(), now());
insert into "public"."t_menu" values (100056, 100053, 'delete', NULL, NULL, 'variable:delete', NULL, '1', '1', NULL, now(), now());
insert into "public"."t_menu" values (100057, 100053, 'depend apps', '/flink/variable/depend_apps', 'flink/variable/DependApps', 'variable:depend_apps', '', '0', '0', NULL, now(), now());
insert into "public"."t_menu" values (100058, 100053, 'show original', NULL, NULL, 'variable:show_original', NULL, '1', '1', NULL, now(), now());
insert into "public"."t_menu" values (100059, 100001, 'view', null, null, 'user:view', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100060, 100037, 'view', null, null, 'token:view', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100061, 100002, 'view', null, null, 'role:view', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100062, 100043, 'view', null, null, 'team:view', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100063, 100047, 'view', null, null, 'permissions:view', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100065, 100014, 'view', null, null, 'project:view', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100066, 100015, 'view', null, null, 'app:view', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100067, 100053, 'view', NULL, NULL, 'variable:view', NULL, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100068, 100033, 'view', null, null, 'setting:view', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100069, 100053, 'depend view', null, null, 'variable:depend_apps', null, '1', '1', NULL, now(), now());
insert into "public"."t_menu" values (100070, 100015, 'savepoint trigger', null, null, 'savepoint:trigger', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100071, 100033, 'link view', null, null, 'externalLink:view', null, '1', '1', NULL, now(), now());
insert into "public"."t_menu" values (100072, 100033, 'link create', null, null, 'externalLink:create', null, '1', '1', NULL, now(), now());
insert into "public"."t_menu" values (100073, 100033, 'link update', null, null, 'externalLink:update', null, '1', '1', NULL, now(), now());
insert into "public"."t_menu" values (100074, 100033, 'link delete', null, null, 'externalLink:delete', null, '1', '1', NULL, now(), now());
insert into "public"."t_menu" values (100075, 100015, 'sql delete', null, null, 'sql:delete', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (100076, 100033, 'add yarn queue', null, null, 'yarnQueue:create', '', '1', '0', null, now(), now());
insert into "public"."t_menu" values (100077, 100033, 'update yarn queue', null, null, 'yarnQueue:update', '', '1', '0', null, now(), now());
insert into "public"."t_menu" values (100078, 100033, 'delete yarn queue', null, null, 'yarnQueue:delete', '', '1', '0', null, now(), now());

-- ----------------------------
-- Records of t_role
-- ----------------------------
insert into "public"."t_role" values (100001, 'developer', 'developer', now(), now(), null);
insert into "public"."t_role" values (100002, 'team admin', 'Team Admin has all permissions inside the team.', now(), now(), null);

-- ----------------------------
-- Records of t_role_menu
-- ----------------------------
insert into "public"."t_role_menu" values (100001, 100001, 100014);
insert into "public"."t_role_menu" values (100002, 100001, 100016);
insert into "public"."t_role_menu" values (100003, 100001, 100017);
insert into "public"."t_role_menu" values (100004, 100001, 100018);
insert into "public"."t_role_menu" values (100005, 100001, 100020);
insert into "public"."t_role_menu" values (100006, 100001, 100021);
insert into "public"."t_role_menu" values (100007, 100001, 100022);
insert into "public"."t_role_menu" values (100008, 100001, 100025);
insert into "public"."t_role_menu" values (100009, 100001, 100026);
insert into "public"."t_role_menu" values (100010, 100001, 100027);
insert into "public"."t_role_menu" values (100011, 100001, 100028);
insert into "public"."t_role_menu" values (100012, 100001, 100029);
insert into "public"."t_role_menu" values (100013, 100001, 100030);
insert into "public"."t_role_menu" values (100014, 100001, 100031);
insert into "public"."t_role_menu" values (100015, 100001, 100032);
insert into "public"."t_role_menu" values (100016, 100001, 100013);
insert into "public"."t_role_menu" values (100017, 100001, 100015);
insert into "public"."t_role_menu" values (100018, 100002, 100014);
insert into "public"."t_role_menu" values (100019, 100002, 100016);
insert into "public"."t_role_menu" values (100020, 100002, 100017);
insert into "public"."t_role_menu" values (100021, 100002, 100018);
insert into "public"."t_role_menu" values (100022, 100002, 100020);
insert into "public"."t_role_menu" values (100023, 100002, 100021);
insert into "public"."t_role_menu" values (100024, 100002, 100022);
insert into "public"."t_role_menu" values (100025, 100002, 100025);
insert into "public"."t_role_menu" values (100026, 100002, 100026);
insert into "public"."t_role_menu" values (100027, 100002, 100027);
insert into "public"."t_role_menu" values (100028, 100002, 100028);
insert into "public"."t_role_menu" values (100029, 100002, 100029);
insert into "public"."t_role_menu" values (100030, 100002, 100030);
insert into "public"."t_role_menu" values (100031, 100002, 100031);
insert into "public"."t_role_menu" values (100032, 100002, 100032);
insert into "public"."t_role_menu" values (100033, 100002, 100013);
insert into "public"."t_role_menu" values (100034, 100002, 100015);
insert into "public"."t_role_menu" values (100035, 100002, 100000);
insert into "public"."t_role_menu" values (100036, 100002, 100036);
insert into "public"."t_role_menu" values (100037, 100002, 100047);
insert into "public"."t_role_menu" values (100038, 100002, 100048);
insert into "public"."t_role_menu" values (100039, 100002, 100049);
insert into "public"."t_role_menu" values (100040, 100002, 100050);
insert into "public"."t_role_menu" values (100041, 100002, 100051);
insert into "public"."t_role_menu" values (100042, 100002, 100052);
insert into "public"."t_role_menu" values (100043, 100002, 100053);
insert into "public"."t_role_menu" values (100044, 100002, 100054);
insert into "public"."t_role_menu" values (100045, 100002, 100055);
insert into "public"."t_role_menu" values (100046, 100002, 100056);
insert into "public"."t_role_menu" values (100047, 100001, 100065);
insert into "public"."t_role_menu" values (100048, 100002, 100065);
insert into "public"."t_role_menu" values (100049, 100001, 100066);
insert into "public"."t_role_menu" values (100050, 100002, 100066);
insert into "public"."t_role_menu" values (100051, 100002, 100063);
insert into "public"."t_role_menu" values (100052, 100002, 100059);
insert into "public"."t_role_menu" values (100053, 100002, 100067);
insert into "public"."t_role_menu" values (100054, 100001, 100053);
insert into "public"."t_role_menu" values (100055, 100001, 100057);
insert into "public"."t_role_menu" values (100056, 100001, 100067);
insert into "public"."t_role_menu" values (100057, 100001, 100069);
insert into "public"."t_role_menu" values (100058, 100002, 100057);
insert into "public"."t_role_menu" values (100059, 100002, 100058);
insert into "public"."t_role_menu" values (100060, 100002, 100069);
insert into "public"."t_role_menu" values (100061, 100002, 100071);
insert into "public"."t_role_menu" values (100062, 100002, 100072);
insert into "public"."t_role_menu" values (100063, 100002, 100073);
insert into "public"."t_role_menu" values (100064, 100002, 100074);
insert into "public"."t_role_menu" values (100065, 100001, 100075);
insert into "public"."t_role_menu" values (100066, 100002, 100075);
insert into "public"."t_role_menu" values (100067, 100002, 100076);
insert into "public"."t_role_menu" values (100068, 100002, 100077);
insert into "public"."t_role_menu" values (100069, 100002, 100078);

-- ----------------------------
-- Records of t_setting
-- ----------------------------
insert into "public"."t_setting" values (1, 'streampark.maven.settings', null, 'Maven Settings File Path', 'Maven Settings.xml 完整路径', 1);
insert into "public"."t_setting" values (2, 'streampark.maven.central.repository', null, 'Maven Central Repository', 'Maven 私服地址', 1);
insert into "public"."t_setting" values (3, 'streampark.maven.auth.user', null, 'Maven Central Repository Auth User', 'Maven 私服认证用户名', 1);
insert into "public"."t_setting" values (4, 'streampark.maven.auth.password', null, 'Maven Central Repository Auth Password', 'Maven 私服认证密码', 1);
insert into "public"."t_setting" values (5, 'alert.email.host', null, 'Alert Email Smtp Host', '告警邮箱Smtp Host', 1);
insert into "public"."t_setting" values (6, 'alert.email.port', null, 'Alert Email Smtp Port', '告警邮箱的Smtp Port', 1);
insert into "public"."t_setting" values (7, 'alert.email.from', null, 'Alert  Email From', '发送告警的邮箱', 1);
insert into "public"."t_setting" values (8, 'alert.email.userName', null, 'Alert  Email User', '用来发送告警邮箱的认证用户名', 1);
insert into "public"."t_setting" values (9, 'alert.email.password', null, 'Alert Email Password', '用来发送告警邮箱的认证密码', 1);
insert into "public"."t_setting" values (10, 'alert.email.ssl', 'false', 'Alert Email Is SSL', '发送告警的邮箱是否开启SSL', 2);
insert into "public"."t_setting" values (11, 'docker.register.address', null, 'Docker Register Address', 'Docker容器服务地址', 1);
insert into "public"."t_setting" values (12, 'docker.register.user', null, 'Docker Register User', 'Docker容器服务认证用户名', 1);
insert into "public"."t_setting" values (13, 'docker.register.password', null, 'Docker Register Password', 'Docker容器服务认证密码', 1);
insert into "public"."t_setting" values (14, 'docker.register.namespace', null, 'Namespace for docker image used in docker building env and target image register', 'Docker命名空间', 1);
insert into "public"."t_setting" values (15, 'ingress.mode.default', null, 'Automatically generate an nginx-based ingress by passing in a domain name', 'Ingress域名地址', 1);

-- ----------------------------
-- Records of t_user
-- ----------------------------
insert into "public"."t_user" values (100000, 'admin', '', 'rh8b1ojwog777yrg0daesf04gk', '2513f3748847298ea324dffbf67fe68681dd92315bda830065facd8efe08f54f', null, 1, null, '1', now(), now(), null, 0, null, null);

-- ----------------------------
-- Records of t_permissions
-- ----------------------------
insert into "public"."t_permissions" values (100000, 100000, 100000, 100001, now(), now());
