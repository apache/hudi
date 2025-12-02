const { themes } = require("prism-react-renderer");
const versions = require("./versions.json");
const { originalSlackUrl } = require('./constants');
const allDocHomesPaths = [
  "/docs/",
  "/docs/next/",
  ...versions.slice(1).map((version) => `/docs/${version}/`),
];

/** @type {import('@docusaurus/types').DocusaurusConfig} */

module.exports = {
  title: "Apache Hudi",
  tagline:
    "Hudi brings transactions, record-level updates/deletes and change streams to data lakes!",
  url: "https://hudi.apache.org",
  baseUrl: "/",
  onBrokenLinks: "throw",
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: "warn",
    },
  },
  favicon: "/assets/images/favicon.ico",
  organizationName: "apache",
  projectName: "hudi",
  customFields: {
    copyrightText:
      "Hudi, Apache and the Apache feather logo are trademarks of The Apache Software Foundation.",
    tagline:
      "Hudi brings transactions, record-level updates/deletes and change streams to data lakes!",
    taglineConfig: {
      prefix: "Apache Hudi™ brings",
      suffix: " to data lakes",
      content: [
        "transactions",
        "row-level updates/deletes",
        "CDC and indexes"
      ],
    },
    slackUrl: originalSlackUrl,
  },
  plugins: [
    [
      "@docusaurus/plugin-content-blog",
      {
        id: "video-blog",
        path: "videoBlog",
        routeBasePath: "videos",
        blogSidebarCount: 0,
        onUntruncatedBlogPosts: "ignore",
        onInlineAuthors: "ignore",
      },
    ],
    [
      "@docusaurus/plugin-content-docs",
      {
        id: "contribute",
        path: "contribute",
        routeBasePath: "contribute",
        sidebarPath: require.resolve("./sidebarsContribute.js"),
        showLastUpdateAuthor: false,
        showLastUpdateTime: false,
      },
    ],
    [
      "@docusaurus/plugin-content-docs",
      {
        id: "community",
        path: "community",
        routeBasePath: "community",
        sidebarPath: require.resolve("./sidebarsCommunity.js"),
        showLastUpdateAuthor: false,
        showLastUpdateTime: false,
      },
    ],
    [
      "@docusaurus/plugin-content-docs",
      {
        id: "releases",
        path: "releases",
        routeBasePath: "releases",
        sidebarPath: require.resolve("./sidebarsReleases.js"),
        showLastUpdateAuthor: false,
        showLastUpdateTime: false,
      },
    ],
    [
      "@docusaurus/plugin-content-docs",
      {
        id: "learn",
        path: "learn",
        routeBasePath: "learn",
        sidebarPath: require.resolve("./sidebarsLearn.js"),
        showLastUpdateAuthor: false,
        showLastUpdateTime: false,
      },
    ],
    [
      "@docusaurus/plugin-client-redirects",
      {
        fromExtensions: ["html"],
        createRedirects: function (path) {
          // redirect to /docs from /docs/introduction,
          // as introduction has been made the home doc
          if (allDocHomesPaths.includes(path)) {
            return [`${path}/quick-start-guide`];
          }
        },
        redirects: [
          {
            from: "/faq",
            to: "/learn/faq",
          },
          {
            from: "/talks",
            to: "/learn/talks",
          },
          {
            from: "/tech-specs",
            to: "/learn/tech-specs",
          },
          {
            from: "/tech-specs-1point0",
            to: "/learn/tech-specs-1point0",
          },
          {
            from: [
              "/docs/contribute",
              "/docs/next/contribute",
              "/contribute/get-involved",
            ],
            to: "/community/get-involved",
          },
          {
            from: ["/contribute/team"],
            to: "/community/team",
          },
          {
            from: ["/docs/next/hoodie_cleaner"],
            to: "/docs/next/cleaning",
          },
          {
            from: ["/docs/next/indexing"],
            to: "/docs/next/indexes",
          },
          {
            from: ["/docs/releases", "/docs/next/releases"],
            to: "/releases/release-1.1.0",
          },
          {
            from: ["/releases"],
            to: "/releases/release-1.1.0",
          },
        ],
      },
    ],
  ],
  themeConfig: {
    metadata: [
      {
        name: "keywords",
        content:
          "apache hudi, data lake, lakehouse, big data, apache spark, apache flink, presto, trino, analytics, data engineering",
      },
    ],
    algolia: {
      apiKey: "e300f1558b703c001c515c0e7f8e0908",
      indexName: "apache_hudi",
      contextualSearch: true,
      appId: "BH4D9OD16A",
    },
    navbar: {
      logo: {
        alt: "Apache Hudi",
        src: "assets/images/hudi.png",
      },
      items: [
        {
          label: "Docs",
          to: "/docs/overview",
        },
        {
          label: "Learn",
          position: "left",
          items: [
            {
              label: "Tutorial Series",
              to: "/learn/tutorial-series",
            },
            {
              label: "Blogs",
              to: "/learn/blog",
            },
            {
              label: "Talks",
              to: "/learn/talks",
            },
            {
              label: "Video Guides",
              to: "/learn/videos",
            },
            {
              label: "FAQ",
              to: "/learn/faq",
            },
            {
              label: "Tech Specs",
              to: "/learn/tech-specs",
            },
            {
              label: "Tech Specs 1.0",
              to: "/learn/tech-specs-1point0",
            },
          ],
        },
        {
          label: "Contribute",
          position: "left",
          items: [
            {
              label: "Developer Sync Call",
              to: "/contribute/developer-sync-call",
            },
            {
              label: "How to Contribute",
              to: "/contribute/how-to-contribute",
            },
            {
              label: "Developer Setup",
              to: "/contribute/developer-setup",
            },
            {
              label: "RFC Process",
              to: "/contribute/rfc-process",
            },
            {
              label: "Security",
              to: "/contribute/security",
            },
          ],
        },
        {
          label: "Community",
          position: "left",
          items: [
            {
              label: "Get Involved",
              to: "/community/get-involved",
            },
            {
              label: "Community Syncs",
              to: "/community/syncs",
            },
            {
              label: "Office Hours",
              to: "/community/office_hours",
            },
            {
              label: "Team",
              to: "/community/team",
            },
            {
              label: 'Github',
              href: 'https://github.com/apache/hudi',
            },
            {
              label: 'Slack',
              href: originalSlackUrl,
            },
            {
              label: 'LinkedIn',
              href: 'https://www.linkedin.com/company/apache-hudi/',
            },
            {
              label: 'YouTube',
              href: 'https://www.youtube.com/@apachehudi',
            },
            {
              label: 'X',
              href: 'https://x.com/ApacheHudi',
            },
          ],
        },
        {
          label: "Ecosystem",
          position: "left",
          items: [
            {
              label: "Adopters",
              to: "/powered-by",
            },
            {
              label: "Roadmap",
              to: "/roadmap",
            },
            {
              label: "Integrations",
              to: "/ecosystem",
            },
          ],
        },
        { to: "/releases/download", label: "Download", position: "left" },
      ],
    },
    footer: {
      style: "light",
      links: [
        {
          title: "About",
          items: [
            {
              label: "Our Vision",
              to: "/blog/2021/07/21/streaming-data-lake-platform",
            },
            {
              label: "Concepts",
              to: "/docs/concepts",
            },
            {
              label: "Team",
              to: "/community/team",
            },
            {
              label: "Releases",
              to: "/releases/release-1.1.0",
            },
            {
              label: "Download",
              to: "/releases/download",
            },
            {
              label: "Adopters",
              to: "powered-by",
            },
          ],
        },
        {
          title: "Learn",
          items: [
            {
              label: "Quick Start",
              to: "/docs/quick-start-guide",
            },
            {
              label: "Tutorial Series",
              to: "/learn/tutorial-series",
            },
            {
              label: "Blog",
              to: "/learn/blog",
            },
            {
              label: "Talks",
              to: "/learn/talks",
            },
            {
              label: "Video Guides",
              to: "/learn/videos",
            },
            {
              label: "FAQ",
              to: "/learn/faq",
            },
          ],
        },
        {
          title: "Hudi On Cloud",
          items: [
            {
              label: "AWS",
              to: "/docs/s3_hoodie",
            },
            {
              label: "Google Cloud",
              to: "/docs/gcs_hoodie",
            },
            {
              label: "Alibaba Cloud",
              to: "/docs/oss_hoodie",
            },
            {
              label: "Microsoft Azure",
              to: "/docs/azure_hoodie",
            },
            {
              label: "Tencent Cloud",
              to: "/docs/cos_hoodie",
            },
            {
              label: "IBM Cloud",
              to: "/docs/ibm_cos_hoodie",
            },
            {
              label: "Oracle Cloud",
              to: "/docs/oci_hoodie",
            },
          ],
        },
        {
          title: "Community",
          items: [
            {
              label: "Get Involved",
              to: "/community/get-involved",
            },
            {
              label: "Slack",
              href: originalSlackUrl,
            },
            {
              label: "GitHub",
              href: "https://github.com/apache/hudi",
            },
            {
              label: "X",
              href: "https://x.com/ApacheHudi",
            },
            {
              label: "YouTube",
              href: "https://www.youtube.com/channel/UCs7AhE0BWaEPZSChrBR-Muw",
            },
            {
              label: "Linkedin",
              href: "https://www.linkedin.com/company/apache-hudi/?viewAsMember=true",
            },
            {
              label: "Mailing List",
              to: "mailto:dev-subscribe@hudi.apache.org?Subject=SubscribeToHudi",
            },
          ],
        },
        {
          title: "Apache",
          items: [
            {
              label: "Events",
              to: "https://www.apache.org/events/current-event",
            },
            {
              label: "Thanks",
              to: "https://www.apache.org/foundation/thanks.html",
            },
            {
              label: "License",
              to: "https://www.apache.org/licenses",
            },
            {
              label: "Security",
              to: "https://www.apache.org/security",
            },
            {
              label: "Privacy",
              to: "/asf/privacy",
            },
            {
              label: "Telemetry",
              to: "/asf/telemetry",
            },
            {
              label: "Sponsorship",
              to: "https://www.apache.org/foundation/sponsorship.html",
            },
            {
              label: "Foundation",
              to: "https://www.apache.org",
            },
          ],
        },
      ],
      logo: {
        alt: "Apache Hudi™",
        src: "/assets/images/hudi.png",
        href: "https://hudi.apache.org/",
      },
      copyright:
        'Copyright © 2021 <a href="https://apache.org">The Apache Software Foundation</a>, Licensed under the <a href="https://www.apache.org/licenses/LICENSE-2.0"> Apache License, Version 2.0</a>. Hudi, Apache and the Apache feather logo are trademarks of The Apache Software Foundation.<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=8f594acf-9b77-44fb-9475-3e82ead1910c" /><img referrerpolicy="no-referrer-when-downgrade" src="https://analytics.apache.org/matomo.php?idsite=47&amp;rec=1" />',
    },
    prism: {
      theme: themes.dracula,
      additionalLanguages: ["java", "scala"],
      prismPath: require.resolve("./src/theme/prism-include-languages.js"),
    },
    announcementBar: {
      id: "announcementBar-2",
      content:
        "⭐️ If you like <b>Apache Hudi</b>, give it a star on <a target=\"_blank\" rel=\"noopener noreferrer\" href=\"https://github.com/apache/hudi\"><b>GitHub!<svg xmlns='http://www.w3.org/2000/svg\\' width='16' height='16' fill='currentColor' class='bi bi-github' viewBox='0 -2 16 16'><path d='M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z'/></svg></b></a> ⭐",
      isCloseable: false,
    },
    colorMode: {
      defaultMode: "light",
      disableSwitch: true,
    },
  },
  presets: [
    [
      "@docusaurus/preset-classic",
      {
        docs: {
          sidebarPath: require.resolve("./sidebars.js"),
          editUrl: ({ versionDocsDirPath, docPath }) => {
              return `https://github.com/apache/hudi/tree/asf-site/website/${versionDocsDirPath}/${docPath}`;
          },
          includeCurrentVersion: true,
          versions: {
            current: {
              label: "Current",
              path: "next",
              banner: "unreleased",
            },
            "1.1.0": {
              label: "1.1.0",
              path: "",
            },
          },
        },
        blog: {
          editUrl: "https://github.com/apache/hudi/edit/asf-site/website/blog/",
          blogTitle: "Blogs List Page",
          blogSidebarCount: 0,
          blogSidebarTitle: "Recent posts",
          /**
           * URL route for the blog section of your site.
           * *DO NOT* include a trailing slash.
           */
          routeBasePath: "blog",
          include: ["*.md", "*.mdx"],
          postsPerPage: 12,
          /**
           * Theme components used by the blog pages.
           */
          blogListComponent: "@theme/BlogListPage",
          blogPostComponent: "@theme/BlogPostPage",
          blogTagsListComponent: "@theme/BlogTagsListPage",
          blogTagsPostsComponent: "@theme/BlogTagsPostsPage",
          feedOptions: {
            type: "all",
            title: "Apache Hudi: User-Facing Analytics",
          },
          showReadingTime: true,
          onUntruncatedBlogPosts: "ignore",
          onInlineAuthors: "ignore",
        },
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
      },
    ],
  ],
  scripts: [
    {
      src: "https://widget.kapa.ai/kapa-widget.bundle.js",
      "data-website-id": "9e4444ba-93cc-45ea-b143-783ae0fbeb6f",
      "data-project-name": "Apache Hudi",
      "data-project-color": "#FFFFFF",
      "data-project-logo": "/assets/images/logo-big.png",
      "data-modal-disclaimer": "This AI assistant answers Apache Hudi questions using your [documentation](https://hudi.apache.org/docs/quick-start-guide/), [dev setup](https://hudi.apache.org/contribute/developer-setup/), the [tech specs](https://hudi.apache.org/tech-specs-1point0/) and open GitHub Issues from the last year.",
      "data-modal-title": "Apache Hudi AI Assistant",
      "data-modal-example-questions-title": "Try asking me...",
      "data-modal-example-questions": "How can I convert an existing COW table to MOR?,How do I set up incremental queries with Hudi tables?",
      "data-modal-image" : "/assets/images/logo-small.png",
      "data-modal-image-ask-ai" : "/assets/images/logo-small.png",
      "data-modal-header-min-height" : "64px",
      "data-modal-image-height": "44",
      "data-modal-image-width": "64",
      "data-modal-header-bg-color": "#f8f9fa",
      "data-modal-title-color": "#0db1f9",
      "data-button-text-color": "#29557a",
      "data-button-text": "Ask AI",
      "data-consent-required": "true",
      "data-consent-screen-title": "Help us improve our AI assistant",
      "data-consent-screen-disclaimer" : "By clicking &quot;Allow tracking&quot;, you consent to the use of the AI assistant in accordance with kapa.ai's [Privacy Policy](https://www.kapa.ai/content/privacy-policy). This service uses reCAPTCHA, which requires your consent to Google's [Privacy Policy](https://policies.google.com/privacy) and [Terms of Service](https://policies.google.com/terms). By proceeding, you explicitly agree to both kapa.ai's and Google's privacy policies.",
      "data-consent-screen-accept-button-text": "Allow tracking",
      "data-modal-disclaimer-font-size" : "0.80rem",
      "data-query-input-placeholder-text-color": "#29557a",
      "data-submit-query-button-bg-color": "#0db1f9",
      "data-query-input-text-color": "#29557a",
      "data-user-analytics-cookie-enabled": "false",
      "data-query-input-border-color": "#211b0e",
      "data-question-text-color": "#0db1f9",
      "data-answer-text-color": "#000",
      "data-thread-clear-button-bg-color": "#000000",
      "data-thread-clear-button-text-color": "#FFFFFF",
      "data-answer-feedback-button-bg-color": "#000000",
      "data-answer-feedback-button-text-color": "#FFFFFF",
      "data-answer-feedback-button-active-bg-color": "#000000",
      "data-answer-feedback-button-active-text-color": "#FFFFFF",
      "data-answer-copy-button-bg-color": "#000000",
      "data-answer-copy-button-text-color": "#FFFFFF",
      "data-example-question-button-text-color": "#29557a",
      "data-modal-disclaimer-bg-color": "#f8f9fa",
      "data-modal-disclaimer-text-color": "#0db1f9",
      "data-deep-thinking-button-bg-color": "#0db1f9",
      "data-deep-thinking-button-text-color": "#FFFFFF",
      "data-deep-thinking-button-active-bg-color": "#0db1f9",
      "data-deep-thinking-button-hover-bg-color": "#FFFFFF",
      "data-deep-thinking-button-active-hover-bg-color": "#29557a",
      "data-deep-thinking-button-active-text-color": "#FFFFFF",
      async: true,
    }
  ]
};
