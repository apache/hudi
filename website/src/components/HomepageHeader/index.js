import React from "react";
import clsx from "clsx";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import styles from "./styles.module.css";
import LinkButton from "@site/src/components/UI/LinkButton";

function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();
  const [firstHalf, secondHaf] = siteConfig.title.split(" ");
  return (
    <header className={clsx("hero hero--primary", styles.heroBanner)}>
      <div className="container">
        <div className={styles.contentWrapper}>
          <div className={styles.content}>
            <img
              className={clsx("hero__img", styles.heroImg)}
              src={require("/assets/images/logo-big.png").default}
              alt="Hudi banner"
            />
            <h1 className={clsx("hero__title", styles.heroTitle)}>
              <span>{firstHalf}</span> {secondHaf}
            </h1>
            <p className="hero__subtitle">{siteConfig.tagline}</p>
            <div className={styles.buttons}>
              <LinkButton to="/releases/release-0.12.0">
                Latest releases
              </LinkButton>
              <LinkButton type="secondary" to="/docs/quick-start-guide">
                Documentation
              </LinkButton>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}

export default HomepageHeader;
