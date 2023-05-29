import React from "react";
import clsx from "clsx";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import styles from "./styles.module.css";
import LinkButton from "@site/src/components/UI/LinkButton";
import FeatureRender from "./FeatureRender";

function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();
  const [firstHalf, secondHaf] = siteConfig.title.split(" ");
  return (
    <header className={clsx("hero hero--primary", styles.heroBanner)}>
      <div className="container">
        <div className={styles.contentWrapper}>
          <div className={styles.content}>
            <div className={styles.leftContent}>
              <h1 className={clsx("hero__title", styles.heroTitle)}>
                <span>{firstHalf}</span> {secondHaf}
              </h1>
              <FeatureRender />
              <div className={styles.buttons}>
                <LinkButton to="/releases/release-0.13.1">
                  Latest releases
                </LinkButton>
                <LinkButton type="secondary" to="/docs/quick-start-guide">
                  Documentation
                </LinkButton>
              </div>
            </div>
            <div className={styles.imageWrapper}>
              <img
                className={clsx("hero__img", styles.heroImg)}
                src={require("/assets/images/logo-big.png").default}
                alt="Hudi banner"
              />
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}

export default HomepageHeader;
