import React from 'react';
import clsx from 'clsx';
import Heading from "@theme/Heading";
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import styles from './index.module.css';
import classnames from "classnames";

const AnchoredH2 = Heading("h2");

function NewReleaseMessage() {
return (
      <div className="container">
       <div className="wrapper">
      <br/>
    </div></div>
  );
}

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
            to="/releases/release-0.12.0">
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
                    Hudi is a rich platform to build streaming data lakes with incremental data pipelines <br/>
                    on a self-managing database layer, while being optimized for lake engines and regular batch processing.
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
                                            Upserts, Deletes with fast, pluggable indexing.
                                        </td>
                                        <td>
                                            <i className={classnames("feather", `icon-zap`)}></i>
                                            Incremental queries, Record level change streams
                                        </td>
                                    </tr>
                                    <tr>
                                        <td>
                                            <i className={classnames("feather", `icon-zap`)}></i>
                                            Transactions, Rollbacks, Concurrency Control.
                                        </td>
                                        <td>
                                            <i className={classnames("feather", `icon-zap`)}></i>
                                            SQL Read/Writes from Spark, Presto, Trino, Hive & more
                                        </td>
                                    </tr>
                                    <tr>
                                        <td>
                                            <i className={classnames("feather", `icon-zap`)}></i>
                                            Automatic file sizing, data clustering, compactions, cleaning.
                                        </td>
                                        <td>
                                            <i className={classnames("feather", `icon-zap`)}></i>
                                            Streaming ingestion, Built-in CDC sources & tools.
                                        </td>
                                    </tr>
                                    <tr>
                                        <td>
                                            <i className={classnames("feather", `icon-zap`)}></i>
                                            Built-in metadata tracking for scalable storage access.
                                        </td>
                                        <td>
                                            <i className={classnames("feather", `icon-zap`)}></i>
                                            Backwards compatible schema evolution and enforcement.
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
      <NewReleaseMessage />
      <HomepageHeader />
      <main>
        <DataLake />
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
