import {useDateTimeFormat} from "@docusaurus/theme-common/internal";
import React, { useState } from 'react';
import Link from '@docusaurus/Link';
import { useBaseUrlUtils } from '@docusaurus/core/lib/client/exports/useBaseUrl';
import AuthorName from '@site/src/components/AuthorName';
import styles from '../ContentList/styles.module.css';

const allBlogPosts = ((ctx) => {
  const blogpostNames = ctx.keys();
  return blogpostNames.reduce(
    (blogposts, blogpostName, i) => {
      try {
        const module = ctx(blogpostName);
        if (module && (module.frontMatter || module.metadata)) {
          return [
            ...blogposts,
            {
              frontMatter: {...(module.frontMatter || {})},
              metadata: {...(module.metadata || {})},
              assets: {...(module.assets || {})}
            },
          ];
        }
      } catch (e) {
        console.warn('Error loading blog post:', blogpostName, e);
      }
      return blogposts;
    },
    []
  );
})(require.context('../../../blog', true, /\.mdx?$/));

const sortedBlogPosts = allBlogPosts
  .filter(post => post.metadata && post.metadata.title && post.metadata.permalink)
  .sort((a,b) => {
    const dateA = a.metadata?.date ? new Date(a.metadata.date).getTime() : 0;
    const dateB = b.metadata?.date ? new Date(b.metadata.date).getTime() : 0;
    return dateB - dateA;
  });

const POSTS_PER_PAGE = 12;

export default function BlogList() {
  const { withBaseUrl } = useBaseUrlUtils();
  const [currentPage, setCurrentPage] = useState(1);

  const totalPages = Math.ceil(sortedBlogPosts.length / POSTS_PER_PAGE);
  const startIndex = (currentPage - 1) * POSTS_PER_PAGE;
  const endIndex = startIndex + POSTS_PER_PAGE;
  const currentPosts = sortedBlogPosts.slice(startIndex, endIndex);

  const handlePageChange = (page) => {
    setCurrentPage(page);
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  const getPageNumbers = () => {
    const pages = [];
    const maxVisible = 5;
    let startPage = Math.max(1, currentPage - Math.floor(maxVisible / 2));
    let endPage = Math.min(totalPages, startPage + maxVisible - 1);

    if (endPage - startPage < maxVisible - 1) {
      startPage = Math.max(1, endPage - maxVisible + 1);
    }

    for (let i = startPage; i <= endPage; i++) {
      pages.push(i);
    }
    return pages;
  };

  return (
    <div className={styles.container}>
      <h2>All Blog Posts</h2>
      <div className={styles.grid}>
        {currentPosts.map((blog, index) => {
          const { frontMatter, assets, metadata } = blog;
          const { date, title, authors, permalink, description } = metadata || {};
          const image = assets?.image ?? frontMatter?.image ?? "/assets/images/hudi.png";

          if (!title || !permalink) {
            return null;
          }

          const dateTimeFormat = useDateTimeFormat({
            day: 'numeric',
            month: 'long',
            year: 'numeric',
            timeZone: 'UTC',
          });
          const formatDate = (blogDate) => dateTimeFormat.format(new Date(blogDate));
          const formattedDate = date ? formatDate(date) : '';

          return (
            <article key={index} className={styles.card}>
              <Link to={permalink} className={styles.link} target="_blank" rel="noopener noreferrer">
                <div className={styles.imageWrapper}>
                  <img
                    src={withBaseUrl(image, { absolute: true })}
                    alt={title}
                    className={styles.image}
                  />
                </div>
                <div className={styles.content}>
                  <div className={styles.meta}>
                    <AuthorName
                      authors={authors}
                      className={styles.authorName}
                      withLink={false}
                    />
                    <span className={styles.date}>{formattedDate}</span>
                  </div>
                  <h3 className={styles.title}>{title}</h3>
                </div>
              </Link>
            </article>
          );
        })}
      </div>

      {totalPages > 1 && (
        <nav className={styles.pagination} aria-label="Blog pagination">
          <button
            className={styles.paginationButton}
            onClick={() => handlePageChange(currentPage - 1)}
            disabled={currentPage === 1}
            aria-label="Previous page"
          >
            Previous
          </button>

          <div className={styles.paginationNumbers}>
            {getPageNumbers().map((pageNum) => (
              <button
                key={pageNum}
                className={`${styles.paginationNumber} ${currentPage === pageNum ? styles.active : ''}`}
                onClick={() => handlePageChange(pageNum)}
                aria-label={`Page ${pageNum}`}
                aria-current={currentPage === pageNum ? 'page' : undefined}
              >
                {pageNum}
              </button>
            ))}
          </div>

          <button
            className={styles.paginationButton}
            onClick={() => handlePageChange(currentPage + 1)}
            disabled={currentPage === totalPages}
            aria-label="Next page"
          >
            Next
          </button>
        </nav>
      )}

      <div className={styles.paginationInfo}>
        Showing {startIndex + 1}-{Math.min(endIndex, sortedBlogPosts.length)} of {sortedBlogPosts.length} posts
      </div>
    </div>
  );
}

