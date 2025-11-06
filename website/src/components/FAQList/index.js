import React from 'react';
import Link from '@docusaurus/Link';
import styles from './styles.module.css';

const faqSections = [
  {
    id: 'general',
    title: 'General FAQ',
    description: 'General questions about Hudi, use cases, and getting started.',
    href: '/faq/general',
  },
  {
    id: 'design_and_concepts',
    title: 'Design & Concepts',
    description: 'Questions about Hudi\'s design, architecture, and core concepts.',
    href: '/faq/design_and_concepts',
  },
  {
    id: 'writing_tables',
    title: 'Writing Tables',
    description: 'Questions about writing data to Hudi tables, upserts, and inserts.',
    href: '/faq/writing_tables',
  },
  {
    id: 'reading_tables',
    title: 'Reading Tables',
    description: 'Questions about reading data from Hudi tables and query types.',
    href: '/faq/reading_tables',
  },
  {
    id: 'table_services',
    title: 'Table Services',
    description: 'Questions about compaction, cleaning, clustering, and other table services.',
    href: '/faq/table_services',
  },
  {
    id: 'storage',
    title: 'Storage',
    description: 'Questions about Hudi storage formats, file layouts, and data organization.',
    href: '/faq/storage',
  },
  {
    id: 'integrations',
    title: 'Integrations',
    description: 'Questions about integrating Hudi with Spark, Flink, Presto, and other systems.',
    href: '/faq/integrations',
  },
];

export default function FAQList() {
  return (
    <div className={styles.faqListContainer}>
      <h2>FAQ Sections</h2>
      <div className={styles.faqGrid}>
        {faqSections.map((section) => (
          <article key={section.id} className={styles.faqCard}>
            <Link to={section.href} className={styles.faqLink} target="_blank" rel="noopener noreferrer">
              <div className={styles.faqContent}>
                <h3 className={styles.faqTitle}>{section.title}</h3>
                <p className={styles.faqDescription}>{section.description}</p>
                <div className={styles.faqAction}>
                  <span className={styles.faqLinkText}>Read more →</span>
                </div>
              </div>
            </Link>
          </article>
        ))}
      </div>
    </div>
  );
}

