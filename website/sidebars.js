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
                'docker_demo'
            ],
        },
        {
            type: 'category',
            label: 'Concepts',
            items: [
                'timeline',
                'table_types',
                'indexing',
                'file_layouts',
                'metadata',
                'write_operations',
                'schema_evolution',
                'key_generation',
                'concurrency_control',
            ],
        },
        {
            type: 'category',
            label: 'How To',
            items: [
                {
                    type: 'category',
                    label: 'SQL',
                    items: [
                        'table_management',
                        'procedures'
                    ],
                },
                'writing_data',
                'hoodie_deltastreamer',
                'querying_data',
                'gcp_bigquery',
                'flink_configuration',
                {
                    type: 'category',
                    label: 'Sync to Metastore',
                    items: [
                        'syncing_aws_glue_data_catalog',
                        'syncing_datahub',
                        'syncing_metastore'
                    ],
                }
            ],
        },
        {
            type: 'category',
            label: 'Services',
            items: [
                'migration_guide',
                'compaction',
                'clustering',
                'async_meta_indexing',
                'hoodie_cleaner',
                'transforms',
                'markers',
                'file_sizing',
                'disaster_recovery',
                'snapshot_exporter',
                'precommit_validator',
            ],
        },
        'configurations',
        {
            type: 'category',
            label: 'Guides',
            items: [
                'query_engine_setup',
                'performance',
                'deployment',
                'cli',
                'metrics',
                'encryption',
                'troubleshooting',
                'tuning-guide',
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
                        'bos_hoodie',
                        'jfs_hoodie'
                    ],
                },
            ],
        },
        'use_cases',
        'faq',
        'privacy',
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
