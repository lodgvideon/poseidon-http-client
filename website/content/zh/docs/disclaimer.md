---
title: 免责声明
weight: 6
---

# 免责声明

poseidon-http-client 是一个年轻的项目，这是它的首个正式版本。在依赖它之前，请先读完本页。

## 无担保

本库以 MIT 许可证发布，**按现状（as is）提供**，不附带任何形式的担保。使用风险由你自行承担。具体条款见 [LICENSE](https://github.com/lodgvideon/poseidon-http-client/blob/main/LICENSE) 文件。

## 安全敏感代码，未经第三方审计

本库从零实现了多个安全敏感协议：QUIC 传输层（RFC 9000/9001/9002）、QUIC 的 TLS 1.3 记录保护、HPACK（RFC 7541）以及 QPACK（RFC 9204）。这些路径均不复用 `net/http`、`quic-go` 或任何其他成熟的协议实现。

已经做了什么：

- 约 200 个按 RFC 章节对应的一致性测试，在 CI 中强制执行。
- 对线路格式解析器进行了模糊测试（fuzzing）。
- 与三个相互独立的 HTTP/3 服务端实现（Caddy/quic-go、nginx、aioquic）在真实 UDP 上完成了互操作验证。
- 完整测试套件在 Go 竞态检测器（race detector）下运行。

**没有**做什么：正式的第三方安全审计。测试与模糊测试能降低缺陷率，但无法证明一个从零实现的协议栈不存在漏洞。

## 用于生产环境之前

不要把本库当作 `net/http` 在安全关键系统中的直接替代品。如果你的部署需要面对不受信任的对端、处理敏感数据，或在其他方面属于安全关键场景，请在采用之前自行审查你所依赖的代码路径——或者等项目积累更多生产环境使用记录之后再考虑。

如果用于对你自己掌控的基础设施做负载生成、测试和工具开发，风险要低得多。这正是本库设计的主要使用场景。

## 报告安全漏洞

如果你发现安全问题，请按 [SECURITY.md](https://github.com/lodgvideon/poseidon-http-client/blob/main/SECURITY.md) 中的说明私下报告。不要为安全漏洞开公开的 GitHub issue。
