/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

module.exports = {
    docs: [
        {
            type: 'category',
            label: 'Getting Started',
            collapsed: false,
            items: [
                'overview',
                'quick-start-guide',
                'flink-quick-start-guide',
                'docker_demo',
                'use_cases',
            ],
        },
        {
            type: 'category',
            label: 'Design & Concepts',
            items: [
                'hudi_stack',
                'timeline',
                'file_layouts',
                'table_types',
                'indexing',
                'write_operations',
                'key_generation',
                'record_payload',
                'schema_evolution',
                'metadata',
                'concurrency_control',
            ],
        },
        {
            type: 'category',
            label: 'Ingestion',
            items: [
                'hoodie_streaming_ingestion',
                'ingestion_flink',
                'ingestion_kafka_connect',
            ],
        },
        {
            type: 'category',
            label: 'Writing Tables',
            items: [
                'sql_ddl',
                'sql_dml',
                'writing_data',
                'writing_tables_streaming_writes',
            ],
        },
        {
            type: 'category',
            label: 'Reading Tables',
            items: [
                'sql_queries',
                'reading_tables_batch_reads',
                'reading_tables_streaming_reads',
            ],
        },
        {
            type: 'category',
            label: 'Table Services',
            items: [
                'migration_guide',
                'compaction',
                'clustering',
                'metadata_indexing',
                'hoodie_cleaner',
                'rollbacks',
                'markers',
                'file_sizing',
                'disaster_recovery',
            ],
        },
        {
            type: 'category',
            label: 'Platform Services',
            items: [
                'snapshot_exporter',
                'precommit_validator',
                'platform_services_post_commit_callback',
                {
                    type: 'category',
                    label: 'Syncing to Catalogs',
                    items: [
                        'syncing_aws_glue_data_catalog',
                        'syncing_datahub',
                        'syncing_metastore',
                        'gcp_bigquery',
                        'syncing_xtable'
                    ],
                }
            ],
        },
        {
            type: 'category',
            label: 'Operations',
            items: [
                'performance',
                'deployment',
                'procedures',
                'cli',
                'metrics',
                'encryption',
                'troubleshooting',
                'tuning-guide',
                'flink_tuning',
            ],
        },
        {
            type: 'category',
            label: 'Configurations',
            items: [
                'basic_configurations',
                'configurations',
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
                        'jfs_hoodie',
                        'oci_hoodie'
                    ],
                },
            ],
        },
        {
            type: 'category',
            label: 'Frequently Asked Questions(FAQs)',
            items: [
                'faq',
                'faq_general',
                'faq_design_and_concepts',
                'faq_writing_tables',
                'faq_reading_tables',
                'faq_table_services',
                'faq_storage',
                'faq_integrations',
            ],
        },
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
            href: 'https://join.slack.com/t/apache-hudi/shared_invite/zt-2ggm1fub8-_yt4Reu9djwqqVRFC7X49g',
        },
    ],
};
