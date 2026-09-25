# 平台能力与边界

核查基线：AstrBot 4.26.2、NapCat 4.18.19，2026-09-25。这表示接口适配依据，并不表示已用正式 QQ 账号完成实测。

## 使用的接口

| OneBot / NapCat 接口 | 用途与依赖字段 |
| --- | --- |
| `get_login_info` | `user_id`：绑定唯一机器人账号 |
| `get_status` | `online`、可选 `good`：写入前确认在线 |
| `get_group_member_info` | `user_id`、`group_id`、`role`、`card`、`join_time`、`title`、`level`、`qq_level`、`shut_up_timestamp` |
| `get_group_detail_info` | NapCat 扩展；`group_all_shut` 为 0/-1/1，新增处理前确认全员禁言 |
| `send_group_msg` | 使用消息段数组构造 `at` 和 `text`，要求返回有效 `message_id` |
| `set_group_ban` | 单位为秒，`duration=0` 解禁；不设置全员禁言 |
| `get_msg` | 核对群、发送者、时间、消息 ID、消息段内容 |
| `delete_msg` | 只撤回已记录并核实身份的提醒 |

不调用修改名片、踢人、全群成员列表、好友遍历或群历史扫描接口。普通群消息仅提供发言触发和名片变化提示，不使用其正文。

## 不应误用的字段

- 群等级 `level=0` 是有效等级；QQ 等级为 0 按缺失处理。只在开启相应保护线时因该等级缺失而暂缓。
- `title_expire_time` 在核查版本中是固定值，不能据此判定头衔过期；按实际非空头衔保护。
- `card_changeable` 在核查版本中固定为 true，不能据此证明群设置允许成员自行改名。
- `no_cache=True` 不构成 QQ 服务器上的原子快照。两次核验和事件代次能降低误操作概率，不能消除平台缓存和外部竞争。

## 通知与消息映射

接收 `group_card`、`group_admin`、`group_ban`、`group_increase`、`group_decrease`。角色/名片/成员身份变化立即使正在准备的请求过期，之后重新核验。

核查版本的名片变化检测会比较缓存名片和群消息中的成员名称。因此被禁言的人改名，不能保证即时收到 `group_card`；后台需要对待处理成员做有预算的补查。对于禁言通知，使用 `operator_id`、`duration`、时间和成员当前截止时间确认归属。

NapCat 的短消息 ID 到原生消息的映射保存在内存缓存中。重启或淘汰可能使 `get_msg` / `delete_msg` 无法定位以前的提醒。插件不会凭相似文本撤回其他消息，也不会在不明结果后反复调用撤回。

## AstrBot 接入

观察处理器注册 `ALL` 事件并使用优先级 100，兼容被封装为消息事件的 OneBot 通知；不阻止其他插件接收普通群消息。命令仅绑定 aiocqhttp 私聊事件并阻止该命令继续进入 LLM。

群配置使用顶层 `template_list`，内部只使用普通字段和一层 `object`。禁言阶梯以分钟列表表示，避免依赖 AstrBot 4.26.2 未在通用字段渲染器中支持的嵌套 `template_list`。

多账号接入时每群指定负责的 QQ。多个平台接入同一 QQ，或一个反向 WebSocket 接入挂多个 QQ，都拒绝自动操作。WebSocket 调用显式携带 `self_id`；切换登录账号需要重载重新绑定。

## 上线检查

先在测试群以“只提醒”运行，确认提醒确实 @ 正确的人、正则与实际名片符合预期、改名后能被补查。再对知情的测试成员短时间启用禁言，验证自助改名权限、通知归属和恢复流程。首次发布的自动测试不执行这些真实群操作。

随机间隔、总量上限和断线冷却解决的是突发请求和重复执行问题；它们不提供“像真人一样就不会被风控”的保证。账号掉线或风控仍需结合 QQ 和 NapCat 原始日志判断。

参考项目：[AstrBot v4.26.2](https://github.com/AstrBotDevs/AstrBot/tree/v4.26.2)、[NapCatQQ](https://github.com/NapNeko/NapCatQQ)。
