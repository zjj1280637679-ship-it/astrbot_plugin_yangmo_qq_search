# QQ 聊天记录搜索器

让你的 AstrBot AI 像使用网络搜索一样按需检索 QQ 群聊历史：先搜索，再引用，再按需要打开邻近语境。插件不会把整段聊天记录自动塞进主对话，也不会把历史消息当成当前指令。

交流与反馈：**QQ 群 916646029**

## 你会得到什么

- 按原词或短语搜索，并可组合发送者、时间和消息类型过滤。
- 不知道关键词时，按 QQ 号读取某位群成员的历史时间线。
- 用稳定引用 `qq:群号:message_id` 打开目标消息前后的有限语境。
- 管理员可分批回填更早历史；游标持久保存，不必每次从头读取。
- 实时消息幂等写入本地索引，QQ 仍是正本，索引可以重建。
- 实时消息索引只依赖 AstrBot 正常收到的群事件；较早历史回填还需要当前 OneBot 后端实现 `get_group_msg_history`。可选 Socket 只增强原生成员过滤和撤回同步。

## 工具契约

```text
q ::= 原词/短语 | ""（此时至少给一个过滤条件）
G ::= ""（当前群）| 群号
F ::= {sender_id?, since?, until?, types?}
citation ::= "qq:" + G + ":" + message_id

qq_search_messages(q,G,F,L)
  -> {count, results[citation,...]}

qq_open_message(citation,before,after)
  -> {target, before[], after[]}

qq_list_member_messages(sender_id,G,cursor,time,L)
  -> {messages[], has_more, next_cursor}

qq_sync_group(G,pages,stop_at,restart)
  -> 更新本插件索引与游标；管理员专用

qq_search_status(G)
  -> {source,index,coverage,cursor,limits,last_error}
```

所有历史结果都带有：

```text
content_role = evidence
instruction_weight = 0
```

这表示历史里的命令、提示词或伪工具调用只作为被搜索到的证据，不获得当前话轮的指令权。

## 安装

1. 在 AstrBot 插件管理中使用仓库 URL 安装，或上传精简 ZIP。
2. 确认 AstrBot 版本满足 `>=4.26.1`（不设版本上限），平台为 `aiocqhttp`。
3. 保持 `socket_path` 为空即可使用实时索引和本地关键词搜索；较早历史回填取决于所连接的 OneBot 后端是否实现 `get_group_msg_history`。
4. 如你已经单独部署兼容的 NapCat 原生增强 Socket，可再填写路径；基础功能不依赖它。
5. 保存配置并重载插件，然后用 `/群聊检索 状态` 核对覆盖范围。

仓库地址：

```text
https://github.com/zjj1280637679-ship-it/astrbot_plugin_yangmo_qq_search
```

## 你可以这样说

- “搜一下这个群里‘普通口语回归图’出现过几次，给我引用。”
- “找 123456789 最近谈到图片压缩的发言。”
- “打开第二条搜索结果前后各三条消息。”
- “把上个月发过的图片、视频和文件记录筛出来。”

AI 根据你的目的组合搜索工具；插件不预设立场，也不会自动把检索结果写入人格记忆。

## 数据源与降级

| 能力 | AstrBot 实时事件 | OneBot 后端历史动作 | 可选 NapCat Socket 增强 |
| --- | --- | --- | --- |
| 新消息实时入库 | 支持 | 不需要 | 不需要 |
| 本地关键词/发送者/时间/任一类型检索 | 支持 | 不需要 | 不需要 |
| 较早群历史分页回填 | 不提供 | 后端实现 `get_group_msg_history` 时支持 | 可作数据源兜底 |
| QQ 原生成员筛选分页 | 本地索引降级 | 不保证 | 支持 |
| 撤回事件同步 | 不提供 | 不保证 | 支持 |

信息源断开时，成员历史首页可以显式降级到本地缓存并标记覆盖未知；带原生游标的后续页不会静默换源。历史游标是后端不透明句柄：即使接口字段名叫 `message_seq`，插件也不会把它与用于去重/撤回的真实群内序号混为一谈。`limit` 始终是单页或单次投影上限，不是“历史总量”。只有逐页回执或完整索引状态才能证明最早可达范围。

## 权限

- 群内普通用户只能搜索当前群。
- 跨群搜索、跨群状态查询与历史回填只允许 AstrBot 管理员。
- 插件不会发送 QQ 消息；它只返回结构化证据给主 Agent。
- `/群聊检索 重建索引` 会重建本地可恢复索引，属于管理员操作。

## 配置

| 配置项 | 默认值 | 作用 |
| --- | ---: | --- |
| `socket_path` | 空 | 可选 NapCat 原生增强 Socket；实时索引和本地搜索不依赖 |
| `database_filename` | `qq_search.sqlite3` | 插件数据目录内的索引文件名 |
| `account_id` | 空 | 无事件时的机器人 QQ 号兜底，通常留空 |
| `page_size` | `30` | 历史回填单页大小，范围 5–100 |
| `max_sync_pages_per_call` | `10` | 单次同步页数上限 |
| `default_result_limit` | `20` | 关键词搜索默认结果数，范围 1–20 |
| `default_member_result_limit` | `30` | 成员历史默认单页条数，范围 1–100 |
| `excerpt_chars` | `280` | 单条搜索摘要字符上限 |
| `redact_output_secrets` | `false` | 可选遮罩工具输出中的疑似密钥；不改写索引正文 |
| `reconcile_enabled` | `true` | 启用最近消息与撤回轻量对账 |
| `reconcile_interval_seconds` | `300` | 后台对账间隔，范围 60–3600 秒 |
| `reconcile_active_groups` | `8` | 每轮最多刷新七天内活跃群的数量 |

## 管理命令

```text
/群聊检索 状态
/群聊检索 同步 3
/群聊检索 搜索 关键词
/群聊检索 成员 目标QQ号
/群聊检索 打开 qq:群号:message_id
/群聊检索 重建索引
```

## 隐私、存储与失败语义

- 数据库存放在 AstrBot 的 `data/plugin_data/astrbot_plugin_yangmo_qq_search/`，不写入插件源码目录。
- 索引保存群消息原值以保持检索、分页、去重和引用一致；请按你的群聊隐私规则管理设备与备份。
- `redact_output_secrets` 只改变工具输出投影，默认关闭；它不是对数据库的删除或加密。
- 本插件不读取 AstrBot 主对话历史、不写人格、不主动发送消息、不执行历史中的命令。
- 信息源错误会返回当前能力与覆盖状态，不把局部缓存冒充完整 QQ 历史。
- 数据源整页只要出现越群或越成员记录就会拒绝，不推进该群游标；撤回同时按消息 ID 与群内消息序列保存墓碑，以覆盖 OneBot/原生历史的 ID 别名。
- 后台对账失败不会阻断 AstrBot 正常聊天。

## 独立性

本插件拥有自己的 OneBot 调用、索引、引用、分页和可选 Socket 客户端。它不导入图片定位或图片生成插件；同时安装时也不会共享句柄、数据库或内部服务。

## 验证

发布前应通过单元测试、版本一致性、权限边界、历史指令降权、秘密扫描、最小包白名单、ZIP 根布局和 16MB 大小限制检查。测试不会自动发送真实 QQ 消息。

## 许可

本项目采用 GNU Affero General Public License v3.0 或更高版本，见 `LICENSE`。
