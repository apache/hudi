import React from 'react';
import clsx from 'clsx';
import Heading from "@theme/Heading";
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import styles from './index.module.css';
import classnames from "classnames";

const AnchoredH2 = Heading("h2");

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <img className="hero__img" src={require('/assets/images/logo-big.png').default} alt="Hudi banner" />
        <h1 className="hero__title">{siteConfig.title}</h1>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/releases/release-0.8.0">
             Latest Releases
          </Link>
          <Link
              className="button button--secondary button--lg"
              to="/docs/quick-start-guide">
              Documentation
          </Link>
        </div>
      </div>
    </header>
  );
}

function DataLake() {
    return (
        <section className="data-lake">
            <div className="container">
                <AnchoredH2 id="hudi-data-lakes" className="text--center">
                    Hudi Data Lakes
                </AnchoredH2>

                <div className="sub-title" className="text--center text--semibold">
                    Hudi brings stream processing to big data, providing fresh data while being an order of magnitude efficient over traditional batch processing.
                </div>

                <img className="hudi-lake text-center" src={require('/assets/images/hudi-lake.png').default} alt="Hudi Data Lake" />
            </div>
        </section>
    );
}

function HomepageFeatures() {
    return (
        <section className="hudi-feature">
            <div className="container">
                <div>
                    <div className="wrapper">
                        <h2 className="text--center">
                            Hudi Features
                        </h2>
                        <div className="hudi-feature-item text--semibold">
                            <table className="features">
                                <tbody>
                                    <tr>
                                        <td>
                                            <i className={classnames("feather", `icon-zap`)}></i>
                                            Upsert support with fast, pluggable indexing.
                                        </td>
                                        <td>
                                            <i className={classnames("feather", `icon-zap`)}></i>
                                            Atomically publish data with rollback support.
                                        </td>
                                    </tr>
                                    <tr>
                                        <td>
                                            <i className={classnames("feather", `icon-zap`)}></i>
                                            Snapshot isolation between writer & queries.
                                        </td>
                                        <td>
                                            <i className={classnames("feather", `icon-zap`)}></i>
                                            Savepoints for data recovery.
                                        </td>
                                    </tr>
                                    <tr>
                                        <td>
                                            <i className={classnames("feather", `icon-zap`)}></i>
                                            Manages file sizes, layout using statistics.
                                        </td>
                                        <td>
                                            <i className={classnames("feather", `icon-zap`)}></i>
                                            Async compaction of row & columnar data.
                                        </td>
                                    </tr>
                                    <tr>
                                        <td>
                                            <i className={classnames("feather", `icon-zap`)}></i>
                                            Timeline metadata to track lineage.
                                        </td>
                                        <td>
                                            <i className={classnames("feather", `icon-zap`)}></i>
                                            Optimize data lake layout with clustering.
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        </section>
    );
}

export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={`Hello from ${siteConfig.title}`}
      description="Description will go into a meta tag in <head />">
      <HomepageHeader />
      <main>
        <DataLake />
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
