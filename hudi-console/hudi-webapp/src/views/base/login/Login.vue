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
<template>
  <div class="h-full overflow-auto">
    <div class="w-full relative h-[calc(100%-120px)] min-h-700px flex items-center section">
      <div class="w-full px-100px mx-auto pb-2">
        <Row :gutter="24">
          <Col :md="12" :span="24" class="self-center pr-5 z-100 -enter-x">
            <LoginSlogan />
          </Col>
          <Col :md="12" :span="24">
            <div
              :class="`${prefixCls}-form`"
              class="relative w-auto m-auto max-w-420px rounded-2px px-10 py-5 shadow-blue-500 enter-x z-100"
              style="border: 2px solid #0db1f9; border-radius: 25px"
            >
              <LoginForm />
            </div>
          </Col>
        </Row>
      </div>
    </div>
    <footer class="w-1150px m-auto text-center bg-transparent opacity-60">
      <a href="https://incubator.apache.org/" target="_blank" class="w-200px m-auto inline-block">
        <img
          src="/@/assets/svg/apache-incubator.svg"
          alt="Apache Incubator logo"
          class="w-200px my-10px mx-auto"
        />
      </a>
      <p class="text-light-100 pt-10px" style="border-top: 1px solid #dad7d7">
        Copyright © 2022 The Apache Software Foundation. Apache StreamPark, StreamPark, and its
        feather logo are trademarks of The Apache Software Foundation.
      </p>
    </footer>
  </div>
</template>
<script lang="ts" setup>
  import LoginForm from './LoginForm.vue';
  import LoginSlogan from './LoginSlogan';
  import { useDesign } from '/@/hooks/web/useDesign';
  import { Row, Col } from 'ant-design-vue';
  defineProps({
    sessionTimeout: {
      type: Boolean,
    },
  });

  // const globSetting = useGlobSetting();
  const { prefixCls } = useDesign('login');
  sessionStorage.removeItem('appPageNo');
  // const title = computed(() => globSetting?.title ?? '');
</script>
<style lang="less">
  @prefix-cls: ~'@{namespace}-login';
  @logo-prefix-cls: ~'@{namespace}-app-logo';
  @countdown-prefix-cls: ~'@{namespace}-countdown-input';
  @active-color: #f8f8f8;

  input.fix-auto-fill,
  .fix-auto-fill input {
    box-shadow: inherit !important;
  }

  .@{prefix-cls}-form {
    .ant-input {
      padding-top: 3px;
      padding-bottom: 3px;

      .ant-input-affix-wrapper:hover,
      .ant-input:not(.ant-input-disabled) {
        border-color: rgba(@active-color, 0.95);
      }
    }

    .text-left {
      .ant-btn {
        padding: 0px;
      }
    }
  }

  [data-theme='dark']{
    .@{prefix-cls}-form{
      .ant-form-item-has-error :not(.ant-input-affix-wrapper-disabled):not(.ant-input-affix-wrapper-borderless).ant-input-affix-wrapper,
      .ant-input-affix-wrapper,
      .ant-input {
        color: rgba(0, 0, 0, 0.85);
        background-color: white;

        &::placeholder {
          color: #bdbdbe !important;
        }
      }
    }
  }
</style>
