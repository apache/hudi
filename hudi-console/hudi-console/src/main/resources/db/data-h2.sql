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
-- Records of t_menu
-- ----------------------------
BEGIN;
INSERT INTO `t_menu` VALUES (100000, 0, 'menu.system', '/system', 'PageView', NULL, 'desktop', '0', 1, 1, '2023-03-30 18:02:38', '2023-03-30 18:02:38');
INSERT INTO `t_menu` VALUES (100001, 100000, 'menu.userManagement', '/system/user', 'system/user/User', NULL, 'user', '0', 1, 1, '2023-03-30 18:02:38', '2023-03-30 18:02:38');
INSERT INTO `t_menu` VALUES (100002, 100000, 'menu.roleManagement', '/system/role', 'system/role/Role', NULL, 'smile', '0', 1, 2, '2023-03-30 18:02:38', '2023-03-30 18:02:38');
INSERT INTO `t_menu` VALUES (100003, 100000, 'menu.menuManagement', '/system/menu', 'system/menu/Menu', 'menu:view', 'bars', '0', 1, 3, '2023-03-30 18:02:38', '2023-03-30 18:02:38');
INSERT INTO `t_menu` VALUES (100004, 100000, 'menu.permissionsManagement', '/system/permissions', 'system/permissions/Permissions', NULL, 'usergroup-add', '0', 1, 2, '2023-03-30 18:02:38', '2023-03-30 18:27:09');
INSERT INTO `t_menu` VALUES (100005, 100001, 'user view', NULL, NULL, 'user:view', NULL, '1', 1, NULL, '2023-03-30 18:02:38', '2023-03-30 18:27:36');
INSERT INTO `t_menu` VALUES (100006, 100001, 'user add', NULL, NULL, 'user:add', NULL, '1', 1, NULL, '2023-03-30 18:02:38', '2023-03-30 18:27:39');
INSERT INTO `t_menu` VALUES (100007, 100001, 'user update', NULL, NULL, 'user:update', NULL, '1', 1, NULL, '2023-03-30 18:02:38', '2023-03-30 18:27:41');
INSERT INTO `t_menu` VALUES (100008, 100001, 'user delete', NULL, NULL, 'user:delete', NULL, '1', 1, NULL, '2023-03-30 18:02:38', '2023-03-30 18:27:44');
INSERT INTO `t_menu` VALUES (100009, 100001, 'user reset', NULL, NULL, 'user:reset', NULL, '1', 1, NULL, '2023-03-30 18:02:38', '2023-03-30 18:27:47');
INSERT INTO `t_menu` VALUES (100010, 100004, 'permissions view', NULL, NULL, 'permissions:view', NULL, '1', 1, NULL, '2023-03-30 18:02:38', '2023-03-30 18:28:42');
INSERT INTO `t_menu` VALUES (100011, 100004, 'permissions add', NULL, NULL, 'permissions:add', NULL, '1', 1, NULL, '2023-03-30 18:02:38', '2023-03-30 18:28:08');
INSERT INTO `t_menu` VALUES (100012, 100004, 'permissions update', NULL, NULL, 'permissions:update', NULL, '1', 1, NULL, '2023-03-30 18:02:38', '2023-03-30 18:28:10');
INSERT INTO `t_menu` VALUES (100013, 100004, 'permissions delete', NULL, NULL, 'permissions:delete', NULL, '1', 1, NULL, '2023-03-30 18:02:38', '2023-03-30 18:28:12');
INSERT INTO `t_menu` VALUES (100014, 100002, 'role view', NULL, NULL, 'role:view', NULL, '1', 1, NULL, '2023-03-30 18:02:38', '2023-03-30 18:38:05');
INSERT INTO `t_menu` VALUES (100015, 100002, 'role add', NULL, NULL, 'role:add', NULL, '1', 1, NULL, '2023-03-30 18:02:38', '2023-03-30 18:28:26');
INSERT INTO `t_menu` VALUES (100016, 100002, 'role update', NULL, NULL, 'role:update', NULL, '1', 1, NULL, '2023-03-30 18:02:38', '2023-03-30 18:28:28');
INSERT INTO `t_menu` VALUES (100017, 100002, 'role delete', NULL, NULL, 'role:delete', NULL, '1', 1, NULL, '2023-03-30 18:02:38', '2023-03-30 18:28:31');
INSERT INTO `t_menu` VALUES (100018, 100003, 'menu view', NULL, NULL, 'menu:view', NULL, '1', 1, NULL, '2023-03-30 18:42:41', '2023-03-30 18:44:35');
INSERT INTO `t_menu` VALUES (100019, 100003, 'menu add', NULL, NULL, 'menu:add', NULL, '1', 1, NULL, '2023-03-30 18:43:41', '2023-03-30 18:44:36');
INSERT INTO `t_menu` VALUES (100020, 100003, 'menu delete', NULL, NULL, 'menu:delete', NULL, '1', 1, NULL, '2023-03-30 18:43:20', '2023-03-30 18:44:36');
INSERT INTO `t_menu` VALUES (100021, 100003, 'menu update', NULL, NULL, 'menu:update', NULL, '1', 1, NULL, '2023-03-30 18:42:58', '2023-03-30 18:44:37');
COMMIT;

-- ----------------------------
-- Records of t_role
-- ----------------------------
BEGIN;
INSERT INTO `t_role` VALUES (100000, 'admin', 'admin', '2023-03-30 18:02:38', '2023-03-30 18:23:04', NULL);
COMMIT;

-- ----------------------------
-- Records of t_role_menu
-- ----------------------------
BEGIN;
INSERT INTO `t_role_menu` VALUES (1, 100000, 100000);
INSERT INTO `t_role_menu` VALUES (2, 100000, 100001);
INSERT INTO `t_role_menu` VALUES (3, 100000, 100002);
INSERT INTO `t_role_menu` VALUES (4, 100000, 100003);
INSERT INTO `t_role_menu` VALUES (5, 100000, 100004);
INSERT INTO `t_role_menu` VALUES (6, 100000, 100005);
INSERT INTO `t_role_menu` VALUES (7, 100000, 100006);
INSERT INTO `t_role_menu` VALUES (8, 100000, 100007);
INSERT INTO `t_role_menu` VALUES (9, 100000, 100008);
INSERT INTO `t_role_menu` VALUES (10, 100000, 100009);
INSERT INTO `t_role_menu` VALUES (11, 100000, 100010);
INSERT INTO `t_role_menu` VALUES (12, 100000, 100011);
INSERT INTO `t_role_menu` VALUES (13, 100000, 100012);
INSERT INTO `t_role_menu` VALUES (14, 100000, 100013);
INSERT INTO `t_role_menu` VALUES (15, 100000, 100014);
INSERT INTO `t_role_menu` VALUES (16, 100000, 100015);
INSERT INTO `t_role_menu` VALUES (17, 100000, 100016);
INSERT INTO `t_role_menu` VALUES (18, 100000, 100017);
INSERT INTO `t_role_menu` VALUES (19, 100000, 100018);
INSERT INTO `t_role_menu` VALUES (20, 100000, 100019);
INSERT INTO `t_role_menu` VALUES (21, 100000, 100020);
INSERT INTO `t_role_menu` VALUES (22, 100000, 100021);
COMMIT;

-- ----------------------------
-- Records of t_user
-- ----------------------------
insert into `t_user` values (100000, 'admin', '', 'rh8b1ojwog777yrg0daesf04gk', '2c7942254a9ab5833affcc1f6bcf32d1d0ad9d535ff90824ea78946ce18a197e', null, 1, '1', now(), now(),null,0,null,null);

-- ----------------------------
-- Records of t_permissions
-- ----------------------------
BEGIN;
INSERT INTO `t_permissions` VALUES (100000, 100000, 100000, '2023-03-30 18:02:38', '2023-03-30 18:29:06');
COMMIT;

