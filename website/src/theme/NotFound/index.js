import React from "react";
import { translate } from "@docusaurus/Translate";
import { PageMetadata } from "@docusaurus/theme-common";
import Layout from "@theme/Layout";
import NotFoundContent from "@theme/NotFound/Content";
import BlogTabs from "../../components/BlogTabs";
import { Route, Switch } from "react-router-dom";

export default function NotFoundWrapper(props) {
  console.log("propsprops", props);

  const title = translate({
    id: "theme.NotFound.title",
    message: "Page Not Found",
  });

  const BlogNotFound = () => (
    <Layout title="Data not found">
      <div className="container" style={{ marginBottom: 16 }}>
        <BlogTabs items={[]} />
      </div>
    </Layout>
  );

  const GeneralNotFound = () => (
    <>
      <Layout>
        <NotFoundContent />
      </Layout>
    </>
  );

  return (
    <>
      <PageMetadata title={title} />
      <Switch>
        {/* Handle specific 404 pages */}
        <Route path="/blog/*" component={BlogNotFound} />

        {/* General 404 page */}
        <Route path="*" component={GeneralNotFound} />
      </Switch>
    </>
  );
}
