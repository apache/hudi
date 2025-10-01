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
                <LinkButton to="/releases/release-1.0.2">
                  Latest releases
                </LinkButton>
                <LinkButton type="secondary" to="/docs/quick-start-guide">
                  Documentation
                </LinkButton>
              </div>
            </div>
              <div className={styles.videoWrapper}>
                <div id={styles.ytContainer} onClick={addElement}></div>
                <div>Clicking on this link will load and send data from and to Google.</div>
              </div>
          </div>
        </div>
      </div>
    </header>
  );
}

export default HomepageHeader;
