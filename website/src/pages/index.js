import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import DataLake from "@site/src/components/DataLakes";
import Events from "@site/src/components/EventFeature";
import HomepageFeatures from "@site/src/components/HomepageFeatures";
import JoinCommunity from "@site/src/components/JoinCommunity";
import WhyHudi from "@site/src/components/WhyHudi";
import HomepageHeader from "@site/src/components/HomepageHeader";
import BlogsSlider from "@site/src/components/BlogsSlider";
import Integrations from "@site/src/components/Integrations";
import styles from './styles.module.css';

export default function Home() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={`Apache Hudi | An Open Source Data Lake Platform`}
      shouldShowOnlyTitle={true}
      description="Description will go into a meta tag in <head />">
        <img src={require("/assets/images/home-background.png").default} alt={'bg-image'} className={'home_background_img'} />
        <HomepageHeader />
         {/*<Events />*/}
        <main>
          <DataLake />
          <Integrations/>
          <HomepageFeatures />
          <WhyHudi />
          <BlogsSlider />
          <JoinCommunity />
        </main>

    </Layout>
  );
}
