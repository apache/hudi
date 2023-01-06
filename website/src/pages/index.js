import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import DataLake from "@site/src/components/DataLakes";
import HomepageFeatures from "@site/src/components/HomepageFeatures";
import JoinCommunity from "@site/src/components/JoinCommunity";
import WhyHudi from "@site/src/components/WhyHudi";
import HomepageHeader from "@site/src/components/HomepageHeader";
import BlogsSlider from "@site/src/components/BlogsSlider";
import styles from './styles.module.css';

function NewReleaseMessage() {
  return (
    <div className="container">
      <div className="wrapper">
        <br />
      </div>
    </div>
  );
}

export default function Home() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={`Hello from ${siteConfig.title}`}
      description="Description will go into a meta tag in <head />"
      wrapperClassName={styles.homePage}
    >
      <NewReleaseMessage />
      <HomepageHeader />
      <main>
        <DataLake />
        <HomepageFeatures />
        <WhyHudi />
        <BlogsSlider />
        <JoinCommunity />
      </main>
    </Layout>
  );
}
