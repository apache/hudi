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
--><template>
  <svg
    :class="[prefixCls, $attrs.class, spin && 'svg-icon-spin']"
    :style="getStyle"
    aria-hidden="true"
  >
    <use :xlink:href="symbolId" />
  </svg>
</template>
<script lang="ts">
  import type { CSSProperties } from 'vue';
  import { defineComponent, computed } from 'vue';
  import { useDesign } from '/@/hooks/web/useDesign';

  export default defineComponent({
    name: 'SvgIcon',
    props: {
      prefix: {
        type: String,
        default: 'icon',
      },
      name: {
        type: String,
        required: true,
      },
      size: {
        type: [Number, String],
        default: 16,
      },
      spin: {
        type: Boolean,
        default: false,
      },
    },
    setup(props) {
      const { prefixCls } = useDesign('svg-icon');
      const symbolId = computed(() => `#${props.prefix}-${props.name}`);

      const getStyle = computed((): CSSProperties => {
        const { size } = props;
        let s = `${size}`;
        s = `${s.replace('px', '')}px`;
        return {
          width: s,
          height: s,
        };
      });
      return { symbolId, prefixCls, getStyle };
    },
  });
</script>
<style lang="less" scoped>
  @prefix-cls: ~'@{namespace}-svg-icon';

  .@{prefix-cls} {
    display: inline-block;
    overflow: hidden;
    vertical-align: -0.15em;
    fill: currentColor;
  }

  .svg-icon-spin {
    animation: loadingCircle 1s infinite linear;
  }
</style>
