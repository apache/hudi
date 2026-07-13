import React from 'react';
import Head from '@docusaurus/Head';

/**
 * Emits schema.org FAQPage JSON-LD. Accepts either `items`
 * ([{question, answer}]) or a pre-serialized `json` string (used by the
 * remark-faq-structured-data plugin on the /faq pages).
 */
export function FAQStructuredData({items, json}) {
  const data = json
    ? JSON.parse(json)
    : {
        '@context': 'https://schema.org',
        '@type': 'FAQPage',
        mainEntity: (items ?? []).map(({question, answer}) => ({
          '@type': 'Question',
          name: question,
          acceptedAnswer: {
            '@type': 'Answer',
            text: answer,
          },
        })),
      };
  return (
    <Head>
      <script type="application/ld+json">{JSON.stringify(data)}</script>
    </Head>
  );
}

/**
 * Visible FAQ section for blog posts, backed by FAQPage structured data.
 * Answers are plain strings so the on-page text and the JSON-LD stay in sync.
 *
 * Usage in a post (no import needed, registered via src/theme/MDXComponents):
 *   ## FAQ
 *   <PostFAQ heading={null} items={[{question: '...', answer: '...'}]} />
 */
export default function PostFAQ({items, heading = 'Frequently Asked Questions'}) {
  if (!items?.length) {
    return null;
  }
  return (
    <section>
      <FAQStructuredData items={items} />
      {heading && <h2>{heading}</h2>}
      {items.map(({question, answer}) => (
        <details key={question} style={{marginBottom: '0.75rem'}}>
          <summary>
            <strong>{question}</strong>
          </summary>
          <p style={{marginTop: '0.5rem'}}>{answer}</p>
        </details>
      ))}
    </section>
  );
}
