import React, { useCallback, useEffect, useState } from "react";
import useEmblaCarousel from "embla-carousel-react";
import clsx from "classnames";

import LinkButton from "@site/src/components/UI/LinkButton";
import Title from "@site/src/components/Title";

import BlogCard from "./BlogCard";
import RightIcon from "./Images/left_slider_icon.svg";
import LeftIcon from "./Images/right_slider_icon.svg";

import styles from "./styles.module.css";
const sliderBlogData = [
  {
    "frontMatter": {
      "title": "Data Lake / Lakehouse Guide: Powered by Data Lake Table Formats (Delta Lake, Iceberg, Hudi)",
      "authors": [
        {
          "name": "Simon Späti"
        }
      ],
      "category": "blog",
      "image": "/assets/images/blog/2022-08-25-Data-Lake-Lakehouse-Guide-Powered-by-Data-Lake-Table-Formats-Delta-Lake-Iceberg-Hudi.png"
    },
    "assets": {
      "authorsImageUrls": [
        null
      ]
    },
    "metadata": {
      "permalink": "/blog/2022/08/25/Data-Lake-Lakehouse-Guide-Powered-by-Data-Lake-Table-Formats-Delta-Lake-Iceberg-Hudi",
      "editUrl": "https://github.com/apache/hudi/edit/asf-site/website/blog/blog/2022-08-25-Data-Lake-Lakehouse-Guide-Powered-by-Data-Lake-Table-Formats-Delta-Lake-Iceberg-Hudi.mdx",
      "source": "@site/blog/2022-08-25-Data-Lake-Lakehouse-Guide-Powered-by-Data-Lake-Table-Formats-Delta-Lake-Iceberg-Hudi.mdx",
      "title": "Data Lake / Lakehouse Guide: Powered by Data Lake Table Formats (Delta Lake, Iceberg, Hudi)",
      "description": "Redirecting... please wait!!",
      "date": "2022-08-25T00:00:00.000Z",
      "formattedDate": "August 25, 2022",
      "tags": [],
      "readingTime": 0.045,
      "truncated": false,
      "authors": [
        {
          "name": "Simon Späti"
        }
      ],
      "nextItem": {
        "title": "Apache Hudi vs Delta Lake vs Apache Iceberg - Lakehouse Feature Comparison",
        "permalink": "/blog/2022/08/18/Apache-Hudi-vs-Delta-Lake-vs-Apache-Iceberg-Lakehouse-Feature-Comparison"
      }
    }
  },
  {
    "frontMatter": {
      "title": "Apache Hudi vs Delta Lake vs Apache Iceberg - Lakehouse Feature Comparison",
      "authors": [
        {
          "name": "Kyle Weller"
        }
      ],
      "category": "blog",
      "image": "/assets/images/blog/2022-08-18-apache_hudi_vs_delta_lake_vs_apache_iceberg_feature_comparison.png"
    },
    "assets": {
      "authorsImageUrls": [
        null
      ]
    },
    "metadata": {
      "permalink": "/blog/2022/08/18/Apache-Hudi-vs-Delta-Lake-vs-Apache-Iceberg-Lakehouse-Feature-Comparison",
      "editUrl": "https://github.com/apache/hudi/edit/asf-site/website/blog/blog/2022-08-18-Apache-Hudi-vs-Delta-Lake-vs-Apache-Iceberg-Lakehouse-Feature-Comparison.mdx",
      "source": "@site/blog/2022-08-18-Apache-Hudi-vs-Delta-Lake-vs-Apache-Iceberg-Lakehouse-Feature-Comparison.mdx",
      "title": "Apache Hudi vs Delta Lake vs Apache Iceberg - Lakehouse Feature Comparison",
      "description": "Redirecting... please wait!!",
      "date": "2022-08-18T00:00:00.000Z",
      "formattedDate": "August 18, 2022",
      "tags": [],
      "readingTime": 0.045,
      "truncated": false,
      "authors": [
        {
          "name": "Kyle Weller"
        }
      ],
      "prevItem": {
        "title": "Data Lake / Lakehouse Guide: Powered by Data Lake Table Formats (Delta Lake, Iceberg, Hudi)",
        "permalink": "/blog/2022/08/25/Data-Lake-Lakehouse-Guide-Powered-by-Data-Lake-Table-Formats-Delta-Lake-Iceberg-Hudi"
      },
      "nextItem": {
        "title": "Use Flink Hudi to Build a Streaming Data Lake Platform",
        "permalink": "/blog/2022/08/12/Use-Flink-Hudi-to-Build-a-Streaming-Data-Lake-Platform"
      }
    }
  },
  {
    "frontMatter": {
      "title": "Use Flink Hudi to Build a Streaming Data Lake Platform",
      "authors": [
        {
          "name": "Chen Yuzhao"
        },
        {
          "name": "Liu Dalong"
        }
      ],
      "category": "blog",
      "image": "/assets/images/blog/2022-08-12-Use-Flink-Hudi-to-Build-a-Streaming-Data-Lake-Platform.png"
    },
    "assets": {
      "authorsImageUrls": [
        null,
        null
      ]
    },
    "metadata": {
      "permalink": "/blog/2022/08/12/Use-Flink-Hudi-to-Build-a-Streaming-Data-Lake-Platform",
      "editUrl": "https://github.com/apache/hudi/edit/asf-site/website/blog/blog/2022-08-12-Use-Flink-Hudi-to-Build-a-Streaming-Data-Lake-Platform.mdx",
      "source": "@site/blog/2022-08-12-Use-Flink-Hudi-to-Build-a-Streaming-Data-Lake-Platform.mdx",
      "title": "Use Flink Hudi to Build a Streaming Data Lake Platform",
      "description": "Redirecting... please wait!!",
      "date": "2022-08-12T00:00:00.000Z",
      "formattedDate": "August 12, 2022",
      "tags": [],
      "readingTime": 0.045,
      "truncated": false,
      "authors": [
        {
          "name": "Chen Yuzhao"
        },
        {
          "name": "Liu Dalong"
        }
      ],
      "prevItem": {
        "title": "Apache Hudi vs Delta Lake vs Apache Iceberg - Lakehouse Feature Comparison",
        "permalink": "/blog/2022/08/18/Apache-Hudi-vs-Delta-Lake-vs-Apache-Iceberg-Lakehouse-Feature-Comparison"
      },
      "nextItem": {
        "title": "How NerdWallet uses AWS and Apache Hudi to build a serverless, real-time analytics platform",
        "permalink": "/blog/2022/08/09/How-NerdWallet-uses-AWS-and-Apache-Hudi-to-build-a-serverless-real-time-analytics-platform"
      }
    }
  },
  {
    "frontMatter": {
      "title": "How NerdWallet uses AWS and Apache Hudi to build a serverless, real-time analytics platform",
      "authors": [
        {
          "name": "Kevin Chun"
        },
        {
          "name": "Dylan Qu"
        }
      ],
      "category": "blog",
      "image": "/assets/images/blog/2022-08-09-How-NerdWallet-uses-AWS-and-Apache-Hudi-to-build-a-serverless-real-time-analytics-platform.png"
    },
    "assets": {
      "authorsImageUrls": [
        null,
        null
      ]
    },
    "metadata": {
      "permalink": "/blog/2022/08/09/How-NerdWallet-uses-AWS-and-Apache-Hudi-to-build-a-serverless-real-time-analytics-platform",
      "editUrl": "https://github.com/apache/hudi/edit/asf-site/website/blog/blog/2022-08-09-How-NerdWallet-uses-AWS-and-Apache-Hudi-to-build-a-serverless-real-time-analytics-platform.mdx",
      "source": "@site/blog/2022-08-09-How-NerdWallet-uses-AWS-and-Apache-Hudi-to-build-a-serverless-real-time-analytics-platform.mdx",
      "title": "How NerdWallet uses AWS and Apache Hudi to build a serverless, real-time analytics platform",
      "description": "Redirecting... please wait!!",
      "date": "2022-08-09T00:00:00.000Z",
      "formattedDate": "August 9, 2022",
      "tags": [],
      "readingTime": 0.045,
      "truncated": false,
      "authors": [
        {
          "name": "Kevin Chun"
        },
        {
          "name": "Dylan Qu"
        }
      ],
      "prevItem": {
        "title": "Use Flink Hudi to Build a Streaming Data Lake Platform",
        "permalink": "/blog/2022/08/12/Use-Flink-Hudi-to-Build-a-Streaming-Data-Lake-Platform"
      },
      "nextItem": {
        "title": "Build Open Lakehouse using Apache Hudi & dbt",
        "permalink": "/blog/2022/07/11/build-open-lakehouse-using-apache-hudi-and-dbt"
      }
    }
  },
  {
    "frontMatter": {
      "title": "Build Open Lakehouse using Apache Hudi & dbt",
      "excerpt": "How to style blog focused projects on teaching how to build an open Lakehouse using Apache Hudi & dbt",
      "author": "Vinoth Govindarajan",
      "category": "blog",
      "image": "/assets/images/blog/hudi_dbt_lakehouse.png"
    },
    "assets": {
      "authorsImageUrls": [
        null
      ]
    },
    "metadata": {
      "permalink": "/blog/2022/07/11/build-open-lakehouse-using-apache-hudi-and-dbt",
      "editUrl": "https://github.com/apache/hudi/edit/asf-site/website/blog/blog/2022-07-11-build-open-lakehouse-using-apache-hudi-and-dbt.md",
      "source": "@site/blog/2022-07-11-build-open-lakehouse-using-apache-hudi-and-dbt.md",
      "title": "Build Open Lakehouse using Apache Hudi & dbt",
      "description": "The focus of this blog is to show you how to build an open lakehouse leveraging incremental data processing and performing field-level updates. We are excited to announce that you can now use Apache Hudi + dbt for building open data lakehouses.",
      "date": "2022-07-11T00:00:00.000Z",
      "formattedDate": "July 11, 2022",
      "tags": [],
      "readingTime": 6.64,
      "truncated": false,
      "authors": [
        {
          "name": "Vinoth Govindarajan"
        }
      ],
      "prevItem": {
        "title": "How NerdWallet uses AWS and Apache Hudi to build a serverless, real-time analytics platform",
        "permalink": "/blog/2022/08/09/How-NerdWallet-uses-AWS-and-Apache-Hudi-to-build-a-serverless-real-time-analytics-platform"
      },
      "nextItem": {
        "title": "Apache Hudi vs Delta Lake - Transparent TPC-DS Lakehouse Performance Benchmarks",
        "permalink": "/blog/2022/06/29/Apache-Hudi-vs-Delta-Lake-transparent-tpc-ds-lakehouse-performance-benchmarks"
      }
    }
  }
];

const BlogsSlider = () => {
  const [emblaRef, emblaApi] = useEmblaCarousel({
    loop: true,
    slidesToScroll: 1,
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
            {sliderBlogData.map((data, i) => (
              <div className={styles.embla__slide} key={i}>
                <BlogCard blog={data} />
              </div>
            ))}
          </div>
        </div>

        <div className={styles.sliderActionsWrapper}>
          <LeftIcon onClick={() => scrollPrev()} className={styles.arrowIcon} />
          <div className={styles.dotsWrapper}>
            {sliderBlogData.map((blog, i) => (
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
