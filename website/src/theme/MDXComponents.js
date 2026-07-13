import MDXComponents from '@theme-original/MDXComponents';
import PostFAQ, {FAQStructuredData} from '@site/src/components/PostFAQ';

// Components available in all MDX content (blog posts, docs, pages)
// without an explicit import.
export default {
  ...MDXComponents,
  PostFAQ,
  FAQStructuredData,
};
