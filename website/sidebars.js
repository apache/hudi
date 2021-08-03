/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

module.exports = {
    docs: [
        'overview',
        {
            type: 'category',
            label: 'Quick Start',
            collapsed: false,
            items: [
                'quick-start-guide',
                'flink-quick-start-guide',
            ],
        },
        'use_cases',
        'writing_data',
        'concurrency_control',
        'querying_data',
        'configurations',
        'performance',
        'deployment',
        {
            type: 'category',
            label: 'Storage Configurations',
            items: [
                'cloud',
                's3_hoodie',
                'gcs_hoodie',
                'oss_hoodie',
                'azure_hoodie',
                'cos_hoodie',
                'ibm_cos_hoodie',
                'bos_hoodie'
            ],
        },
        {
            type: 'category',
            label: 'Resources',
            items: [
                'docker_demo',
                'metrics',
                'privacy'
            ],
        },
    ],
    quick_links: [
        {
            type: 'link',
            label: 'Powered By',
            href: 'powered-by',
        },
        {
            type: 'link',
            label: 'Chat with us on Slack',
            href: 'https://join.slack.com/t/apache-hudi/shared_invite/enQtODYyNDAxNzc5MTg2LTE5OTBlYmVhYjM0N2ZhOTJjOWM4YzBmMWU2MjZjMGE4NDc5ZDFiOGQ2N2VkYTVkNzU3ZDQ4OTI1NmFmYWQ0NzE',
        },
    ],
};
