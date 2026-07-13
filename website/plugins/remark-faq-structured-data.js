const path = require('path');

// Plain-text rendering of an mdast subtree (questions/answers for JSON-LD).
function textOf(node) {
  if (!node) {
    return '';
  }
  if (node.type === 'text' || node.type === 'inlineCode') {
    return node.value;
  }
  if (node.type === 'code') {
    return node.value;
  }
  if (Array.isArray(node.children)) {
    return node.children.map(textOf).join(node.type === 'list' ? ' ' : '');
  }
  return '';
}

/**
 * Remark plugin that turns the `### Question` + prose structure of the
 * src/pages/faq/*.md pages into schema.org FAQPage JSON-LD, by appending a
 * <FAQStructuredData json="..."/> element (resolved through the global
 * MDX components map). Content pages outside src/pages/faq are untouched.
 */
module.exports = function remarkFaqStructuredData() {
  const faqDir = path.join('src', 'pages', 'faq') + path.sep;
  return (tree, file) => {
    const filePath = file.path || (file.history && file.history[0]) || '';
    if (!filePath.includes(faqDir)) {
      return;
    }

    const faqs = [];
    let current = null;
    for (const node of tree.children) {
      if (node.type === 'heading' && node.depth === 3) {
        current = {question: textOf(node).trim(), answer: []};
        faqs.push(current);
      } else if (node.type === 'heading') {
        current = null;
      } else if (current) {
        const text = textOf(node).trim();
        if (text) {
          current.answer.push(text);
        }
      }
    }

    const items = faqs.filter((faq) => faq.question && faq.answer.length);
    if (!items.length) {
      return;
    }

    const json = JSON.stringify({
      '@context': 'https://schema.org',
      '@type': 'FAQPage',
      mainEntity: items.map(({question, answer}) => ({
        '@type': 'Question',
        name: question,
        acceptedAnswer: {
          '@type': 'Answer',
          text: answer.join('\n\n'),
        },
      })),
    });

    tree.children.push({
      type: 'mdxJsxFlowElement',
      name: 'FAQStructuredData',
      attributes: [{type: 'mdxJsxAttribute', name: 'json', value: json}],
      children: [],
    });
  };
};
