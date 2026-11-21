# 第三方组件声明（Third-Party Notices）

`serialhub` 本项目采用 **MIT License**（见 [LICENSE](LICENSE)）。
以下为随本项目**内置分发**或**依赖**的第三方组件及其许可证。谨向原作者致谢。

## 一、随仓库内置分发的代码（vendored）

以下文件直接内置于 [`serialhub/static/vendor/`](serialhub/static/vendor/)，随本仓库一起分发：

| 组件 | 版本 | 许可证 | 上游 | 内置文件 |
|---|---|---|---|---|
| `@xterm/xterm` | 5.5.0 | MIT | <https://github.com/xtermjs/xterm.js> | `xterm.min.js`、`xterm.min.css` |
| `@xterm/addon-fit` | 0.10.0 | MIT | <https://github.com/xtermjs/xterm.js> | `addon-fit.min.js` |
| `zmodem.js` | 0.1.10 | Apache-2.0 | <https://github.com/FGasper/zmodemjs> | `zmodem.min.js`（与 crc-32 打包） |
| `crc-32` | 1.2.2 | Apache-2.0 | <https://github.com/SheetJS/js-crc32> | 打包进 `zmodem.min.js` |

xterm.js 两项均为 **MIT License**，版权归 xterm.js 作者所有
（`Copyright (c) 2017-2024 The xterm.js authors`）。
MIT 允许再分发，条件是保留版权与许可声明——本文件即为此声明。

`zmodem.js`（作者 Gasper Software Consulting，见其 package.json）与 `crc-32`（Copyright (C) 2014-present SheetJS LLC）均为
**Apache License 2.0**（<https://www.apache.org/licenses/LICENSE-2.0>）；未作修改，仅打包压缩。
`zmodem.min.js` 由锁定在 `tests/zmodem_client/package-lock.json` 的源码一次性构建：

```sh
cd tests/zmodem_client && npm ci
npx --yes esbuild@0.24.2 node_modules/zmodem.js/src/zmodem_browser.js --bundle --minify --format=iife \
  --target=es2017 --legal-comments=inline \
  "--banner:js=/*! zmodem.js 0.1.10 (Apache-2.0, FGasper/zmodemjs) + crc-32 1.2.2 (Apache-2.0, SheetJS) - browser bundle built with esbuild 0.24.2, see THIRD_PARTY_NOTICES.md */" \
  --outfile=../../serialhub/static/vendor/zmodem.min.js
```

本地文件加载失败时回退到 CDN：`https://cdn.jsdelivr.net/npm/zmodem.js@0.1.10/dist/zmodem.js`。
同目录的 `zmodem-shim.js` 是 serialhub 自写的 lrzsz 兼容补丁（MIT，属本项目），不是 zmodem.js 的一部分。

## 二、运行时依赖（经包管理器安装，未内置分发）

| 组件 | 许可证 |
|---|---|
| `pyserial` | BSD-3-Clause |
| `fastapi` | MIT |
| `uvicorn` | BSD-3-Clause |
| `websockets` | BSD-3-Clause |
| `PyYAML` | MIT |
| `mcp`（官方 Python SDK） | MIT |

## 三、开发 / 测试依赖

| 组件 | 许可证 |
|---|---|
| `pytest` | MIT |
| `httpx` | BSD-3-Clause |
| `playwright`（浏览器 e2e） | Apache-2.0 |

## 四、外部字体（运行时由 CDN 加载，未随仓库分发）

Web UI 通过 Google Fonts CDN 加载 **IBM Plex Sans / IBM Plex Mono**
（**SIL Open Font License 1.1**）。这些字体**不包含**在本仓库中；
CDN 不可达时界面自动回退到系统字体。

---

> **许可证兼容性**：以上全部为宽松许可证（MIT / BSD / Apache-2.0 / OFL），与本项目的 MIT 许可证兼容。
