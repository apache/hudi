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
  <span :class="getTagClass" v-if="getShowTag">{{ getContent }}</span>
</template>
<script lang="ts">
  import type { Menu } from '/@/router/types';

  import { defineComponent, computed } from 'vue';

  import { useDesign } from '/@/hooks/web/useDesign';
  import { propTypes } from '/@/utils/propTypes';

  export default defineComponent({
    name: 'SimpleMenuTag',
    props: {
      item: {
        type: Object as PropType<Menu>,
        default: () => ({}),
      },
      dot: propTypes.bool,
      collapseParent: propTypes.bool,
    },
    setup(props) {
      const { prefixCls } = useDesign('simple-menu');

      const getShowTag = computed(() => {
        const { item } = props;

        if (!item) return false;

        const { tag } = item;
        if (!tag) return false;

        const { dot, content } = tag;
        if (!dot && !content) return false;
        return true;
      });

      const getContent = computed(() => {
        if (!getShowTag.value) return '';
        const { item, collapseParent } = props;
        const { tag } = item;
        const { dot, content } = tag!;
        return dot || collapseParent ? '' : content;
      });

      const getTagClass = computed(() => {
        const { item, collapseParent } = props;
        const { tag = {} } = item || {};
        const { dot, type = 'error' } = tag;
        const tagCls = `${prefixCls}-tag`;
        return [
          tagCls,

          [`${tagCls}--${type}`],
          {
            [`${tagCls}--collapse`]: collapseParent,
            [`${tagCls}--dot`]: dot || props.dot,
          },
        ];
      });
      return {
        getTagClass,
        getShowTag,
        getContent,
      };
    },
  });
</script>
