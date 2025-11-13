import React from "react";
import clsx from "clsx";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import styles from "./styles.module.css";
import LinkButton from "@site/src/components/UI/LinkButton";
import FeatureRender from "./FeatureRender";

function loadScript() {
    return new Promise((resolve, reject) => {
        let script = document.createElement('script');
        script.src = 'https://www.youtube.com/iframe_api';
        script.addEventListener('load', resolve);
        script.addEventListener('error', (e) => reject(e));
        document.body.appendChild(script);
    });
}

function addElement() {
    loadScript().then(() => {
        window.YT.ready(function() {
            let player = new YT.Player(styles.ytContainer, {
            videoId: 'AYaw06_Xazo',
            playerVars: {
                'playsinline': 1
            },
            events: {
                'onReady': (event) => {
                    event.target.playVideo();
                }
            }
            });
        });
    });
}

function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();
  const {
    taglineConfig: { prefix }
  } = siteConfig.customFields;
  const [firstHalf, secondHaf] = prefix.split('™');
  return (
    <header className={clsx("", styles.heroBanner)}>
      <div className="container">
        <div className={styles.contentWrapper}>
          <div className={styles.content}>
            <div className={styles.leftContent}>
              <h1 className={clsx("hero__title", styles.heroTitle)}>
                {firstHalf}
                <sup className={styles.textTm}>TM</sup>
                {secondHaf}
              </h1>
              <FeatureRender/>
              <div className={styles.buttons}>
                <LinkButton to="/releases/release-1.0.2">
                  Get Started
                </LinkButton>
                <LinkButton type="secondary" to="/docs/quick-start-guide">
                  Read Docs
                </LinkButton>
              </div>
            </div>
            <div className={styles.videoWrapper}>
              <img src={require("/assets/images/hudi-logo.png").default} alt={'hudi-logo'}/>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}

export default HomepageHeader;
