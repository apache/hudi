import React from "react";
import {useBaseUrlUtils} from "@docusaurus/core/lib/client/exports/useBaseUrl";

import useLongPress from "@site/src/hooks/useLongPress";
import AuthorName from "@site/src/components/AuthorName";

import styles from "./styles.module.css";

const BlogCard = ({ blog }) => {
  const { withBaseUrl } = useBaseUrlUtils();
  const {
    frontMatter,
    assets,
    metadata,
  } = blog;
  const {
    formattedDate,
    title,
    authors,
  } = metadata;
  const image = assets.image ?? frontMatter.image ?? '/assets/images/hudi.png';
  const onClick = () => {
    window.location.href = blog.link;
  };

  const longPressEvent = useLongPress(() => {}, onClick, {
    shouldPreventDefault: true,
    delay: 100,
  });

  return (
    <div className={styles.blogsWrapper}>
      <div className={styles.cardBlogs} {...longPressEvent}>
        <div>
          <div className={styles.blogImgWrapper}>
            <img src={withBaseUrl(image, {
              absolute: true,
            })} alt={title} className={styles.blogImg} />
          </div>
          <div className={styles.blogContent}>
            <AuthorName authors={authors} className={styles.authorName} />
            <div className={styles.date}>{formattedDate}</div>

          </div>
          <h2 className={styles.blogTitle}>{title}</h2>
        </div>
      </div>
    </div>
  );
};
export default BlogCard;
