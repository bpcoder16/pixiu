#!/bin/sh
# 固定生成工具版本，生成物纳入源码，普通构建和测试无需安装生成工具。
#
# 安装 protoc 36.2（各平台统一下载预编译包）：
#   1. 打开 https://github.com/protocolbuffers/protobuf/releases/tag/v36.2 。
#   2. 选择与操作系统、CPU 架构匹配的 protoc 压缩包；macOS 可选 osx-universal_binary。
#   3. 解压到任意目录，将解压后的 bin 目录加入 PATH；Windows 在环境变量 Path 中添加。
#      macOS/Linux 示例：解压到 "$HOME/.local/protoc-36.2"，执行：
#        export PATH="$HOME/.local/protoc-36.2/bin:$PATH"
#
# 安装 Go 插件（需先安装 Go，各平台均执行）：
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
# 将 go env GOBIN 对应目录加入 PATH；若为空，则使用 go env GOPATH 对应目录下的 bin。
# macOS/Linux 未自定义 GOBIN 时，可执行：
#   export PATH="$(go env GOPATH)/bin:$PATH"
# 使用 zsh 时，将上述适用的 export 行加入 ~/.zshrc，新终端即可找到工具；
# 其他 Shell 使用各自的启动配置文件。
#
# 安装后检查版本：protoc --version 应为 libprotoc 36.2，
# protoc-gen-go --version 应为 protoc-gen-go v1.36.12。
# 在本脚本所在目录运行 ./generate.sh，或在仓库根目录运行 go generate ./biz/realtime。
set -eu
test "$(protoc --version)" = "libprotoc 36.2" || {
    echo "需要 protoc 36.2" >&2
    exit 1
}
test "$(protoc-gen-go --version)" = "protoc-gen-go v1.36.12" || {
    echo "需要 protoc-gen-go v1.36.12" >&2
    exit 1
}
protoc --go_out=. --go_opt=paths=source_relative protocol/realtime.proto
