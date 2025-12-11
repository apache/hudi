import {useDateTimeFormat} from "@docusaurus/theme-common/internal";
import React, {useState, useMemo, useEffect, useRef} from 'react';
import Link from '@docusaurus/Link';
import { useBaseUrlUtils } from '@docusaurus/core/lib/client/exports/useBaseUrl';
import AuthorName from '@site/src/components/AuthorName';
import styles from '../ContentList/styles.module.css';
import {useHistory} from '@docusaurus/router';
import SearchIcon from './Icon/search.svg';
import Title from "@site/src/components/Title";
import { useLocation } from 'react-router-dom';


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

const POSTS_PER_PAGE = 12;

export default function BlogList() {
  const history = useHistory();
  const location = useLocation();
  const urlParams = new URLSearchParams(location.search);
  const defaultCategory = urlParams.get("category");
  const defaultPage = +urlParams.get("page");
  const defaultSearch = urlParams.get("search") || "";

  const { withBaseUrl } = useBaseUrlUtils();
  const [currentPage, setCurrentPage] = useState(defaultPage || 1);
  const [category, setCategory] = useState(defaultCategory || 'all');
  const [searchInput, setSearchInput] = useState(defaultSearch);
  const [searchQuery, setSearchQuery] = useState(defaultSearch);
  const debounceTimerRef = useRef(null);

  const buildUrl = (category, pageNum, search) => {
    const parts = [];
    parts.push(`category=${encodeURIComponent(category)}`);
    parts.push(`page=${pageNum}`);
    if(search && search.trim()) parts.push(`search=${encodeURIComponent(search.trim())}`);
    return `/learn/blog?${parts.join('&')}`;
  };

  useEffect(() => {
    debounceTimerRef.current = setTimeout(() => {
      const isNewSearch = searchInput !== searchQuery;
      if (isNewSearch) {
        setCurrentPage(1);
      }
      setSearchQuery(searchInput);
      history.replace(
        buildUrl(
          category,
          isNewSearch ? 1 : currentPage,
          searchInput
        )
      );

    }, 800);

    return () => {
      if (debounceTimerRef.current) {
        clearTimeout(debounceTimerRef.current);
      }
    };
  }, [searchInput]);

  const filteretdBlogPosts = useMemo(() => {
    let filtered = allBlogPosts;
    // Filter by subCategory
    if(category !== 'all') {
      filtered = filtered.filter((elem) => elem.frontMatter.subCategory === category);
    }

    // Filter by search query - only search by title (case-insensitive)
    if(searchQuery.trim()) {
      const query = searchQuery.toLowerCase().trim();
      filtered = filtered.filter((post) => {
        const title = post.metadata?.title?.toLowerCase() || '';
        return title.includes(query);
      });
    }

    return filtered;
  },[category, searchQuery])

  const sortedBlogPosts = filteretdBlogPosts
    .filter(post => post.metadata && post.metadata.title && post.metadata.permalink)
    .sort((a,b) => {
      const dateA = a.metadata?.date ? new Date(a.metadata.date).getTime() : 0;
      const dateB = b.metadata?.date ? new Date(b.metadata.date).getTime() : 0;
      return dateB - dateA;
    });

  const totalPages = Math.ceil(sortedBlogPosts.length / POSTS_PER_PAGE);
  const startIndex = (currentPage - 1) * POSTS_PER_PAGE;
  const endIndex = startIndex + POSTS_PER_PAGE;
  const currentPosts = sortedBlogPosts.slice(startIndex, endIndex);

  const handlePageChange = (page) => {
    setCurrentPage(page);
    window.scrollTo({ top: 0, behavior: 'smooth' });
    history.push(buildUrl(category, page, searchQuery));
  };

  const handleSearchChange = (e) => {
    setSearchInput(e.target.value);
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

  const categoryData = [
    {label:"All", value:"all"},
    {label:"Indexing", value:"indexing"},
    {label:"Security", value:"security"},
    {label:"Use Case", value:"use case"},
    {label:"Data Lake", value:"data lake"},
    {label:"Lakehouse", value:"lakehouse"},
    {label:"Upserts", value:"upserts"},
  ]


  return (
    <div className={styles.container}>
      <div className={styles.blogTitle}>
        <Title primaryText="Blog"/>
      </div>
      <div className={styles.blogFilterSection}>
        <div className={styles.blogFilters}>
          <div className={styles.categoryBar}>
            {categoryData.map(elem => (
              <button
                key={elem.label}
                className={elem.value === (category ?? 'all') ? styles.categoryActive : styles.category}
                onClick={() => {
                  setCategory(elem.value)
                  setCurrentPage(1)
                  setSearchInput('')
                  setSearchQuery('')
                  history.push(buildUrl(elem.value, 1, ''));
                }}
                type="button"
              >
                {elem.label}
              </button>
            ))}
          </div>
          <div className={styles.searchContainer}>
            <SearchIcon className={styles.searchIcon} width={20} height={20} />
            <input
              type="text"
              className={styles.searchInput}
              placeholder="Search"
              value={searchInput}
              onChange={handleSearchChange}
            />
          </div>
        </div>
      </div>
      <div key={`${category}-${searchQuery}-${currentPage}`} className={styles.gridWrapper}>
        <div className={styles.grid}>
        {currentPosts.map((blog, index) => {
          const { frontMatter, assets, metadata } = blog;
          const { date, title, authors, permalink, description } = metadata || {};
          const image = assets?.image ?? frontMatter?.image ?? "/assets/images/hudi.png";

          if (!title || !permalink)  return null;

          const dateTimeFormat = useDateTimeFormat({
            day: 'numeric',
            month: 'long',
            year: 'numeric',
            timeZone: 'UTC',
          });

          const formatDate = (blogDate) => dateTimeFormat.format(new Date(blogDate));
          const formattedDate = date ? formatDate(date) : '';

          return (
            <article
              key={index}
              className={styles.card}
              style={{ animationDelay: `${index * 0.1}s` }}
            >
              <Link to={permalink} className={styles.link} target="_blank" rel="noopener noreferrer">
                <div className={styles.imageWrapper}>
                  <img
                    src={withBaseUrl(image, { absolute: true })}
                    alt={title}
                    className={styles.image}
                  />
                </div>
                <div className={styles.content}>
                  <h3 className={styles.title}>{title}</h3>
                  <div className={styles.meta}>
                    <AuthorName
                      authors={authors}
                      className={styles.authorName}
                      withLink={false}
                    />
                    <span className={styles.date}>{formattedDate}</span>
                  </div>
                </div>
              </Link>
            </article>
          );
        })}
        </div>
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
        {currentPosts.length > 0 ? (
          <>Showing {startIndex + 1}-{Math.min(endIndex, sortedBlogPosts.length)} of {sortedBlogPosts.length} posts</>
        ) : (
          <>No blog posts available</>
        )}
      </div>
    </div>
  );
}

