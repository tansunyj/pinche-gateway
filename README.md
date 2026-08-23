# Silievo 多协议 LLM 网关中转平台

接收 OpenAI / Anthropic / Responses / Vertex(Gemini) 协议请求，统一转换为内部标准模型后路由到多家上游渠道，并完成 **API Key 认证、余额预占/结算、8 种计费模式、双层请求日志、用户折扣/套餐** 等平台能力。

- 技术栈：Spring Boot 3.3.4 + WebFlux（全链路响应式非阻塞）、Java 21、R2DBC MySQL、Redis Reactive、Log4j2
- 依赖：本机 MySQL（库 `pt_carpool`）+ Redis（localhost:6379）

## 相关仓库

本平台共分 **3 个子项目**，各对应一个独立 Git 仓库：

| 子项目（GitHub 仓库名） | 说明 | Git 地址 |
| ------ | ---- | -------- |
| `pinche-frontend` | 前端 | `https://github.com/tansunyj/pinche-frontend.git` |
| `pinche-backend` | 后端服务 | `https://github.com/tansunyj/pinche-backend.git` |
| `pinche-gateway` | 网关代理 | `https://github.com/tansunyj/pinche-gateway.git` |

> 本仓库是其中的 **`pinche-gateway`（网关代理）** 子项目（本地目录 `gateway`）。三个仓库相互独立，需分别 `git clone` / `git push`；跨仓库协作时各自独立提交、互不影响。

## 构建与运行

```bash
mvn clean compile                  # 编译
mvn clean package -DskipTests      # 打包
java -jar target/silievo-api-gateway-service-1.0-SNAPSHOT.jar   # 运行（默认 dev 环境，端口 3003）
```

> 注意：网关 jar 必须在**终端前台**运行（本机 360 安全卫士会杀后台启动的 java 进程）。

## 配置与密钥（重要）

`application.yml` / `application-dev.yml` 含真实密钥（阿里云 OSS AccessKey、MySQL 密码等），**已 gitignore，不会推送到本仓库**。仓库中上传的是**占位符模板**：

| 本地真实文件（gitignore，不上传） | 仓库上传的模板 |
|---|---|
| `src/main/resources/application.yml` | `src/main/resources/application.yml.example` |
| `src/main/resources/application-dev.yml` | `src/main/resources/application-dev.yml.example` |

**首次克隆 / 全新环境**：从模板生成本地配置文件，并填入你自己的真实值：

```powershell
Copy-Item src/main/resources/application.yml.example src/main/resources/application.yml
Copy-Item src/main/resources/application-dev.yml.example src/main/resources/application-dev.yml
# 编辑 application.yml       → 填入 OSS AccessKey / AccessKeySecret、Bucket、CDN 域名
# 编辑 application-dev.yml  → 填入本地 MySQL root 密码
```

### 需要配置的密钥项

| 配置项 | 说明 |
|---|---|
| `oss.access-key-id` | 阿里云 OSS AccessKey ID（生图 base64→URL 上传用） |
| `oss.access-key-secret` | 阿里云 OSS AccessKey Secret |
| `oss.bucket` / `oss.cdn-base-url` | 对应你的 OSS Bucket 与访问域名 |
| `spring.r2dbc.password`（dev） | 本地 MySQL root 密码 |

> 安全约定：**任何真实密钥都不写入会被 git 跟踪的文件**。本地配置文件保持 gitignore 状态即可；新增密钥配置项时请同步更新对应 `.example` 模板（用 `${ENV_VAR}` 占位），不要在模板里写真实值。

### 环境变量注入（可选）

不想把密钥写死在 yml 里时，可用环境变量占位，Spring 会解析 `${...}`：

```yaml
# application.yml
oss:
  access-key-id: ${OSS_ACCESS_KEY_ID}
  access-key-secret: ${OSS_ACCESS_KEY_SECRET}
```

```powershell
$env:OSS_ACCESS_KEY_ID='你的AK'; $env:OSS_ACCESS_KEY_SECRET='你的SK'; $env:MYSQL_PASSWORD='123456'
java -jar target/silievo-api-gateway-service-1.0-SNAPSHOT.jar
```

## 主要端点

| 端点 | 说明 |
|---|---|
| `/v1/chat/completions` | OpenAI Chat 协议 |
| `/v1/messages` | Anthropic 协议 |
| `/v1/responses` | OpenAI Responses 协议 |
| `/v1/models` | 模型列表 |
| `/v1/images/generations`、`/v1/images/edits` | 生图 / 图片编辑 |
| `/v1/videos/generations` | 生视频（异步任务） |
| `/v1/embeddings/text_embeddings` 等 | 向量（轻量透传） |

详细协议转换、计费规则与踩坑约定见 `CLAUDE.md`；设计文档见 `docs/`。
