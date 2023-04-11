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

create database if not exists `streampark` character set utf8mb4 collate utf8mb4_general_ci;
use streampark;

set names utf8mb4;
set foreign_key_checks = 0;

-- ----------------------------
-- Table structure for t_menu
-- ----------------------------
drop table if exists `t_menu`;
create table `t_menu` (
  `menu_id` bigint not null auto_increment comment 'menu/button id',
  `parent_id` bigint not null comment 'parent menu id',
  `menu_name` varchar(50) collate utf8mb4_general_ci not null comment 'menu button name',
  `path` varchar(255) collate utf8mb4_general_ci default null comment 'routing path',
  `component` varchar(255) collate utf8mb4_general_ci default null comment 'routing component component',
  `perms` varchar(50) collate utf8mb4_general_ci default null comment 'authority id',
  `icon` varchar(50) collate utf8mb4_general_ci default null comment 'icon',
  `type` char(2) collate utf8mb4_general_ci default null comment 'type 0:menu 1:button',
  `display` tinyint collate utf8mb4_general_ci not null default 1 comment 'whether the menu is displayed',
  `order_num` int default null comment 'sort',
  `create_time` datetime not null default current_timestamp comment 'create time',
  `modify_time` datetime not null default current_timestamp on update current_timestamp comment 'modify time',
  primary key (`menu_id`) using btree
) engine=innodb auto_increment=100000 default charset=utf8mb4 collate=utf8mb4_general_ci;

-- ----------------------------
-- Table structure for t_role
-- ----------------------------
drop table if exists `t_role`;
create table `t_role` (
  `role_id` bigint not null auto_increment comment 'user id',
  `role_name` varchar(50) collate utf8mb4_general_ci not null comment 'user name',
  `remark` varchar(100) collate utf8mb4_general_ci default null comment 'remark',
  `create_time` datetime not null default current_timestamp comment 'create time',
  `modify_time` datetime not null default current_timestamp on update current_timestamp comment 'modify time',
  `role_code` varchar(255) collate utf8mb4_general_ci default null comment 'role code',
  primary key (`role_id`) using btree
) engine=innodb auto_increment=100000 default charset=utf8mb4 collate=utf8mb4_general_ci;

-- ----------------------------
-- Table structure for t_role_menu
-- ----------------------------
drop table if exists `t_role_menu`;
create table `t_role_menu` (
  `id` bigint not null auto_increment,
  `role_id` bigint not null,
  `menu_id` bigint not null,
  primary key (`id`) using btree,
  unique key `un_role_menu_inx` (`role_id`,`menu_id`) using btree
) engine=innodb auto_increment=100000 default charset=utf8mb4 collate=utf8mb4_general_ci;

-- ----------------------------
-- Table structure for t_user
-- ----------------------------
drop table if exists `t_user`;
create table `t_user` (
  `user_id` bigint not null auto_increment comment 'user id',
  `username` varchar(255) collate utf8mb4_general_ci not null comment 'user name',
  `nick_name` varchar(50) collate utf8mb4_general_ci not null comment 'nick name',
  `salt` varchar(255) collate utf8mb4_general_ci default null comment 'salt',
  `password` varchar(128) collate utf8mb4_general_ci not null comment 'password',
  `email` varchar(128) collate utf8mb4_general_ci default null comment 'email',
  `user_type` int  not null comment 'user type 1:admin 2:user',
  `status` char(1) collate utf8mb4_general_ci not null comment 'status 0:locked 1:active',
  `create_time` datetime not null default current_timestamp comment 'create time',
  `modify_time` datetime not null default current_timestamp on update current_timestamp comment 'modify time',
  `last_login_time` datetime default null comment 'last login time',
  `sex` char(1) collate utf8mb4_general_ci default null comment 'gender 0:male 1:female 2:confidential',
  `avatar` varchar(100) collate utf8mb4_general_ci default null comment 'avatar',
  `description` varchar(100) collate utf8mb4_general_ci default null comment 'description',
  primary key (`user_id`) using btree,
  unique key `un_username` (`username`) using btree
) engine=innodb auto_increment=100000 default charset=utf8mb4 collate=utf8mb4_general_ci;


-- ----------------------------
-- Table structure for t_permissions
-- ----------------------------
drop table if exists `t_permissions`;
create table `t_permissions` (
  `id` bigint not null auto_increment,
  `user_id` bigint not null comment 'user id',
  `role_id` bigint not null comment 'role id',
  `create_time` datetime not null default current_timestamp comment 'create time',
  `modify_time` datetime not null default current_timestamp on update current_timestamp comment 'modify time',
  primary key (`id`) using btree,
  unique key `un_user_team_role_inx` (`user_id`,`role_id`) using btree
) engine=innodb auto_increment=100000 default charset=utf8mb4 collate=utf8mb4_general_ci;


set foreign_key_checks = 1;
