# 第三方依赖说明

貔貅自身采用 [Apache License 2.0](LICENSE)。第三方依赖及其内部组件保留各自的许可证，貔貅的授权不替代它们的许可条件。

依赖及版本以 [go.mod](go.mod) 为准，通过 Go modules 引入；本仓库当前没有 vendor 目录。本文件提供使用与分发提示，不汇总所有依赖的许可原文，也不替代实际发行包所需的声明。

## 使用与分发

分发包含第三方代码的源码、二进制或容器镜像时，按实际包含的组件保留所需许可、版权和归属声明：

- **MIT / BSD**：保留要求的版权、许可条件和免责声明；BSD 的二进制分发要求在随附文档或其他材料中保留这些内容。
- **Apache-2.0**：附许可证，保留相关声明，按要求标明修改，并保留上游适用的 NOTICE。
- **MPL-2.0**：编译并对外分发含受许可代码的软件时，须让接收者能够取得对应受覆盖源码，并告知获取方式。修改过的受覆盖文件仍按 MPL 提供源码；不包含 MPL 代码的独立业务文件可以闭源。

同一依赖的不同文件或内部组件可能采用不同许可证，不能仅凭模块根许可概括全部代码。具体条件以各上游许可证和源码声明为准。

## MPL-2.0 依赖

当前以下依赖采用 MPL-2.0：

| 模块 | 版本 | 对应版本源码 |
| --- | --- | --- |
| `github.com/go-sql-driver/mysql` | `v1.9.3` | [官方源码压缩包](https://codeload.github.com/go-sql-driver/mysql/zip/refs/tags/v1.9.3) |
| `github.com/hashicorp/go-version` | `v1.8.0` | [官方源码压缩包](https://codeload.github.com/hashicorp/go-version/zip/refs/tags/v1.8.0) |

未修改时可使用对应版本的上游源码获取方式；随产品分发时应确认其可用，必要时自行提供副本。修改后须提供实际使用的受覆盖源码，不能仅指向未修改的上游版本；仅附许可证也不能替代源码提供义务。

依赖变更时同步更新相关说明；分发含依赖的软件时，由分发者按实际组件准备所需声明及源码获取信息。

## 官方说明

- [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)
- [Mozilla MPL-2.0 FAQ](https://www.mozilla.org/en-US/MPL/2.0/FAQ/)
