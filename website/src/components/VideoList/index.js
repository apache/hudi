import React, { useState } from 'react';
import Link from '@docusaurus/Link';
import { useBaseUrlUtils } from '@docusaurus/core/lib/client/exports/useBaseUrl';
import AuthorName from '@site/src/components/AuthorName';
import styles from '../ContentList/styles.module.css';

const allVideos = ((ctx) => {
  const videoNames = ctx.keys();
  return videoNames.reduce(
    (videos, videoName, i) => {
      try {
        const module = ctx(videoName);
        if (module && (module.frontMatter || module.metadata)) {
          return [
            ...videos,
            {
              frontMatter: {...(module.frontMatter || {})},
              metadata: {...(module.metadata || {})},
              assets: {...(module.assets || {})}
            },
          ];
        }
      } catch (e) {
        console.warn('Error loading video:', videoName, e);
      }
      return videos;
    },
    []
  );
})(require.context('../../../videoBlog', true, /\.mdx?$/));

const sortedVideos = allVideos
  .filter(video => video.metadata && video.metadata.title && video.metadata.permalink)
  .sort((a,b) => {
    const dateA = a.metadata?.date ? new Date(a.metadata.date).getTime() : 0;
    const dateB = b.metadata?.date ? new Date(b.metadata.date).getTime() : 0;
    return dateB - dateA;
  });

const POSTS_PER_PAGE = 12;

export default function VideoList() {
  const { withBaseUrl } = useBaseUrlUtils();
  const [currentPage, setCurrentPage] = useState(1);
  
  const totalPages = Math.ceil(sortedVideos.length / POSTS_PER_PAGE);
  const startIndex = (currentPage - 1) * POSTS_PER_PAGE;
  const endIndex = startIndex + POSTS_PER_PAGE;
  const currentVideos = sortedVideos.slice(startIndex, endIndex);

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
      <h2>All Video Guides</h2>
      <div className={styles.grid}>
        {currentVideos.map((video, index) => {
          const { frontMatter, assets, metadata } = video;
          const { date, title, authors, permalink, description } = metadata || {};
          const image = assets?.image ?? frontMatter?.image ?? "/assets/images/hudi.png";
          const videoUrl = frontMatter?.navigate || permalink;
          
          if (!title || !videoUrl) {
            return null;
          }
          
          const dateObj = date ? new Date(date) : null;
          const formattedDate = dateObj ? dateObj.toLocaleDateString('en-US', { 
            year: 'numeric', 
            month: 'long', 
            day: 'numeric' 
          }) : '';

          const isExternalUrl = videoUrl && (videoUrl.startsWith('http://') || videoUrl.startsWith('https://'));
          const LinkComponent = isExternalUrl ? 'a' : Link;
          const linkProps = isExternalUrl 
            ? { href: videoUrl, target: '_blank', rel: 'noopener noreferrer' }
            : { to: videoUrl, target: '_blank', rel: 'noopener noreferrer' };

          return (
            <article key={index} className={styles.card}>
              <LinkComponent {...linkProps} className={styles.link}>
                <div className={styles.imageWrapper}>
                  <img
                    src={withBaseUrl(image, { absolute: true })}
                    alt={title}
                    className={styles.image}
                  />
                </div>
                <div className={styles.content}>
                  <div className={styles.meta}>
                    {authors && authors.length > 0 && (
                      <AuthorName
                        authors={authors}
                        className={styles.authorName}
                        withLink={false}
                      />
                    )}
                    <span className={styles.date}>{formattedDate}</span>
                  </div>
                  <h3 className={styles.title}>{title}</h3>
                  {description && (
                    <p className={styles.description}>{description}</p>
                  )}
                </div>
              </LinkComponent>
            </article>
          );
        })}
      </div>
      
      {totalPages > 1 && (
        <nav className={styles.pagination} aria-label="Video pagination">
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
        Showing {startIndex + 1}-{Math.min(endIndex, sortedVideos.length)} of {sortedVideos.length} videos
      </div>
    </div>
  );
}

