import React, { useCallback, useEffect, useState } from "react";
import useEmblaCarousel from "embla-carousel-react";
import clsx from "classnames";

import LinkButton from "@site/src/components/UI/LinkButton";
import Title from "@site/src/components/Title";

import BlogCard from "./BlogCard";
import RightIcon from "./Icons/left_slider_icon.svg";
import LeftIcon from "./Icons/right_slider_icon.svg";

import styles from "./styles.module.css";

const allPosts = ((ctx) => {
  const blogpostNames = ctx.keys();
  return blogpostNames.reduce(
      (blogposts, blogpostName, i) => {
        const module = ctx(blogpostName);
        return [
          ...blogposts,
          {
            frontMatter: {...module.frontMatter},
            metadata: {...module.metadata},
            assets: {...module.assets}
          },
        ];
      },
      []
  );
})(require.context('../../../blog', true));

const sortedPosts = allPosts.sort((a,b) => new Date(a.metadata.date).getTime() - new Date(b.metadata.date).getTime()).reverse();
const latestPosts = [...sortedPosts.slice(0, 10)];

const BlogsSlider = () => {
  const [emblaRef, emblaApi] = useEmblaCarousel({
    loop: true,
    slidesToScroll: 1,
    align: 0
  });
  const [activeIndex, setActiveIndex] = useState(0);

  const gotoBlock = (id) => {
    emblaApi && emblaApi.scrollTo(id);
  };

  const scrollPrev = useCallback(
    () => emblaApi && emblaApi.scrollPrev(),
    [emblaApi]
  );

  const scrollNext = useCallback(
    () => emblaApi && emblaApi.scrollNext(),
    [emblaApi]
  );

  const onSelect = useCallback(() => {
    if (!emblaApi) return;
    setActiveIndex(emblaApi.selectedScrollSnap());
  }, [emblaApi]);

  useEffect(() => {
    if (!emblaApi) return;
    onSelect();
    emblaApi.on("select", onSelect);
  }, [emblaApi, onSelect]);

  return (
    <section className={styles.cardBlogWrapper}>
      <div className="container">
        <div className={styles.titleWrapper}>
          <Title primaryText="Hudi" secondaryText="Blogs" />
        </div>

        <div className={styles.embla} ref={emblaRef}>
          <div className={styles.embla__container}>
            {latestPosts.map((data, i) => (
              <div className={styles.embla__slide} key={i}>
                <BlogCard blog={data} />
              </div>
            ))}
          </div>
        </div>

        <div className={styles.sliderActionsWrapper}>
          <LeftIcon onClick={() => scrollPrev()} className={styles.arrowIcon} />
          <div className={styles.dotsWrapper}>
            {latestPosts.map((blog, i) => (
              <div
                key={i}
                role="button"
                onClick={() => gotoBlock(i)}
                className={clsx(styles.sliderDots, {
                  [styles.activeSliderDots]: activeIndex === i,
                })}
              />
            ))}
          </div>
          <RightIcon
            onClick={() => scrollNext()}
            className={styles.arrowIcon}
          />
        </div>
        <div className={styles.blogViewAllBtnWrapper}>
          <LinkButton to="/blog" className={styles.blogBtn}>
            View All
          </LinkButton>
        </div>
      </div>
    </section>
  );
};
export default BlogsSlider;
