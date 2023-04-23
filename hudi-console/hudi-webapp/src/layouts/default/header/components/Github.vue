<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<script lang="ts" setup name="Github">
  import { Tooltip } from 'ant-design-vue';
  import Icon from '/@/components/Icon';
  import { GITHUB_URL } from '/@/settings/siteSetting';
</script>
<template>
  <Tooltip title="Github" placement="bottom" :mouseEnterDelay="0.5">
    <a :href="GITHUB_URL" target="_blank" class="text-black hover:text-black github">
      <Icon icon="ant-design:github-outlined" class="!pl-8px" style="font-size: 19px !important" />
      <img
        src="https://img.shields.io/github/stars/apache/incubator-streampark.svg?sanitize=true"
        class="px-5px hidden md:block"
      />

      <img
        src="https://img.shields.io/github/forks/apache/incubator-streampark.svg?sanitize=true"
        class="pr-8px hidden md:block"
      />
    </a>
  </Tooltip>
</template>
<style lang="less">
  [data-theme='dark'] {
    .github,
    .github:hover {
      color: #fff;
    }
  }
</style>
