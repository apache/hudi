<div align="center">
    <br/>
    <h1>
        <a href="https://streampark.apache.org" target="_blank" rel="noopener noreferrer">
        <img width="600" src="https://streampark.apache.org/image/logo_name.png" alt="StreamPark logo">
        </a>
    </h1>
    <strong style="font-size: 1.5rem">Make stream processing easier!!!</strong>
</div>

<br/>

<p align="center">
  <img src="https://tokei.rs/b1/github/apache/streampark">
  <img src="https://img.shields.io/github/v/release/apache/streampark.svg">
  <img src="https://img.shields.io/github/stars/apache/streampark">
  <img src="https://img.shields.io/github/forks/apache/streampark">
  <img src="https://img.shields.io/github/issues/apache/streampark">
  <img src="https://img.shields.io/github/downloads/apache/streampark/total.svg">
  <img src="https://img.shields.io/github/languages/count/apache/streampark">
  <a href="https://www.apache.org/licenses/LICENSE-2.0.html"><img src="https://img.shields.io/badge/license-Apache%202-4EB1BA.svg"></a>
</p>

<div align="center">

**[官网](https://streampark.apache.org)**

</div>

#### [English](README.md) | 中文

# Apache Hudi

Make stream processing easier

> 一个神奇的框架，让流处理更简单。

![](https://streampark.apache.org/image/dashboard.png)

### 使用 Gitpod

在 Gitpod（适用于 GitHub 的免费在线开发环境）中打开项目，并立即开始编码.

[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://github.com/apache/incubator-streampark)

## 安装使用

- Node.js

webui 是用 Javascript 编写的。如果你没有 Node.js 开发环境，请[设置](https://nodejs.org/en/download/)它。需要的最低版本是 14.0。

- 设置镜像源

如果你遇到依赖下载缓慢需要配置 npm 镜像源,方法为在用户文件夹下找到 .npmrc 将以下内容写入

```bash
strict-peer-dependencies=false
save-workspace-protocol = rolling
registry = 'http://registry.npm.taobao.org'
sass_binary_site='http://npm.taobao.org/mirrors/node-sass/'
phantomjs_cdnurl='http://npm.taobao.org/mirrors/phantomjs'
CHROMEDRIVER_CDNURL='http://npm.taobao.org/mirrors/chromedriver'
electron_mirror='http://npm.taobao.org/mirrors/electron/'
fsevents_binary_host_mirror='http://npm.taobao.org/mirrors/fsevents/'
```

- 安装依赖

```bash
pnpm install
# or
yarn install
```

- 运行

```bash
pnpm serve
# or
yarn serve
```

- 打包

```bash
pnpm build
# or
yarn build
```

## 浏览器支持

本地开发推荐使用`Chrome 80+` 浏览器

支持现代浏览器, 不支持 IE

| [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/edge/edge_48x48.png" alt=" Edge" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)</br>IE | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/edge/edge_48x48.png" alt=" Edge" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)</br>Edge | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/firefox/firefox_48x48.png" alt="Firefox" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)</br>Firefox | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/chrome/chrome_48x48.png" alt="Chrome" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)</br>Chrome | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/safari/safari_48x48.png" alt="Safari" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)</br>Safari |
| :-: | :-: | :-: | :-: | :-: |
| not support | last 2 versions | last 2 versions | last 2 versions | last 2 versions |
