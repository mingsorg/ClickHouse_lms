# 日志专用版本 v1 实施计划

日期：2026-10-18。状态：PLAN，尚未实施。本文为可评审的版本范围、接口、任务与验收计划。

## 1. 产品目标与约束

实现面向追加日志的本地存储/查询服务，支持原生HTTP日志接口，并增加Splunk HEC、Elasticsearch Bulk接入适配器。采用ClickHouse 26.9.21.9稳定版作为首个可重复基线，后续升级必须固定版本并重跑验证；不使用浮动latest镜像。

输入特性：仅追加、不做业务更新；单个来源内通常按时间有序，跨来源近似有序但尚未测量乱序分布。不能据此假定全局有序。

业务目标：原始100TB/天，保留10天，最多10台8vCPU/32GB，日志查询100QPS、<100ms。全部保留为目标，不擅自降采样、缩短保留或减少副本以宣称达标。磁盘网络、副本要求、查询类与延迟百分位待确认；当前不承诺原资源能满足目标。测试默认报告端到端p50/p95/p99，不用平均值替代尾延迟。

## 2. 路线：专用服务与引擎分开演进

第一条交付线是可运行日志服务：接入适配、规范化、可靠写入、批处理、查询API和性能证据，先运行在官方ClickHouse上。

第二条交付线是可选的ClickHouse专用发行版：固定上游标签、按功能清单裁剪依赖，保留完整性与升级路径。只有基线证明某项引擎开销是瓶颈，才进入C++补丁阶段。这样可以把网关优化与引擎收益分开归因。

v1不重写存储引擎，不承诺SPL、Elasticsearch Query DSL或Kibana兼容，不实现业务update/delete、不支持任意SQL开放到业务侧。行政保留期回收与业务删除分开定义，任何测试均不直接删除已有工作区文件。

## 3. 架构与进程

客户端/代理 → 原生/HEC/Bulk HTTP适配器 → 认证与限流 → 规范化与校验 → 有界批处理 → ClickHouse Native写入 → MergeTree/ReplicatedMergeTree。

查询API → 租户授权与查询预算 → 参数化查询 → ClickHouse → 游标与结果。

建议Go实现网关和查询服务，复用成熟ClickHouse驱动与连接池；仅在需要引擎补丁时使用C++。首版一个可执行文件支持ingest/query角色，避免每种协议一套存储链路。不要在10台限制外暗中增加未核算的队列集群。

基础可靠模式：客户端支持重试；网关在取得约定的ClickHouse写入确认后，才返回最终成功。网关重启前尚未确认的请求由客户端重试。超时可能已提交，因此是至少一次语义，不能承诺无重复。首版不通过易失内存提前确认、不自造通用分布式消息系统。

可选后续模式：需要提前接受并抗下游中断时，加入有界持久化队列。必须先定义队列复制、fsync、磁盘配额与满盘策略；网关本地WAL仅能承诺已测的进程/主机故障范围，单盘WAL不能承诺整机/磁盘永久损坏不丢失。普通HEC/原生接口可区分accepted和committed；Bulk成功语义不能悄悄改成仅进入队列。

## 4. 规范化数据契约

| 字段 | 作用 |
|---|---|
| tenant_id / dataset | 从凭据授权范围和服务端映射确定，不信任客户端自报租户 |
| event_time | 事件时间，UTC；记录解析结果与异常状态 |
| ingest_time | 网关接收时间，UTC，用于乱序分析与摄入延迟 |
| source_id / source_session / source_sequence | 来源、会话、可选顺序；仅在来源可提供时使用，不能伪造严格顺序 |
| event_id | 服务端生成；可选来源幂等键单独定义 |
| host / service / severity | 热路径的已知结构化字段 |
| trace_id / span_id | 关联检索字段，缺失允许为空或明确默认语义 |
| message | 可检索日志正文 |
| attributes | 未提升字段的有界结构化载荷；控制数量、深度、长度和动态路径 |
| protocol / external_index / source_type | 原始入口及来源元数据 |
| parse_status / schema_version | 解析与契约版本 |

控制单事件、单请求、解压后字节、嵌套深度、字段数及请求并发。非法时间不静默改成当前时间；策略可配置为拒绝，或保留原始时间值并标记fallback。首次解析时间不可用时可用ingest_time作为明确记录的查询时间。

保留期先以event_time+10天作为待确认候选；旧于保留边界的补传需明确拒绝/隔离政策，不能回200后立刻清理。若业务实际上要求接收后10天，则改用ingest_time并重新评估布局。未来时间需限制，避免异常时钟导致长期留存。

属性载荷可先采用有界JSON字符串存储并提升热字段，动态JSON列作为对比实验，避免预先为所有字段建立子列和索引。原始载荷是否完整重复存储单独核算，不默认存两份message。

## 5. 接入协议与兼容矩阵

### 原生API

POST /v1/logs：版本化JSON/NDJSON追加接口，认证、字节上限、gzip与批量上限。请求可选幂等标识但首版不得在尚无持久化判重机制时承诺exactly-once。响应包含request_id、accepted/committed计数和明确定义的错误。允许的部分成功策略必须写入OpenAPI。

### HEC，v0.1纳入event模式

支持POST /services/collector及/services/collector/event，Authorization: Splunk <token>，event为字符串/对象，以及time、host、source、sourcetype、index、fields映射；支持官方event对象拼接的批量格式。index只能映射到凭据允许的dataset，不能创建任意SQL表。

event对象message映射、fields优先级、时间精度与默认值形成固定测试用例。首版先整体校验有界请求，避免流式前缀已写入后才发现后缀非法而无法解释；和Splunk的错误/部分处理差异明确列为兼容限制。响应外形、HTTP与HEC code通过官方样本及指定客户端验证，不仅curl能发成功。

/services/collector/raw列为v0.2：需定义换行事件切分、编码和多行日志处理；不能把多行堆栈自动切坏。未实现前明确拒绝。

/services/collector/ack列为v0.2：channel、ackID、状态持久化、隔离、过期和跨网关路由都需实现。状态映射至本服务已定义的存储确认/副本确认等级；不将内存入队映射成true，不声称与Splunk所有内部语义一致。启用ACK的客户端在未实现时明确报错，不能返回虚假的ackID。ACK故障/过期/重复轮询语义以官方文档和目标客户端做契约测试。

HEC成功不自动保证exactly-once，客户端超时重试仍可能重复。

### Bulk，v0.1只承诺追加子集

支持POST /_bulk与/{dataset}/_bulk，NDJSON framing、末尾换行、action/source配对、gzip、items顺序与errors聚合；逐项保留原始序号，允许有效项成功、无效项失败。

首版支持不带_id的index动作并生成ID。对带_id的index拒绝：官方语义涉及覆盖，而本产品仅追加。create动作首版明确拒绝，直到提供可持久化、跨并发且重启后仍正确的唯一性判定；不能用MergeTree排序键或异步ReplacingMergeTree冒充实时唯一约束。update/delete及版本控制、pipeline、refresh、routing、wait_for_active_shards等未实现参数明确返回不支持，不能忽略后宣称兼容。

这种子集能否直接适配现有代理，必须通过其实际版本和配置验证。很多日志代理使用create或自带_id，需要在明确允许时配置成无_id的index，或者扩展正确的语义；不能保证所有Elastic Agent/Beats/Logstash无需修改即可接入。

Bulk单项成功仅在约定写入确认后返回；上层HTTP 200仍可能errors=true。请求级超限、认证与整体framing错误走请求级错误；单项schema/不支持动作走item错误。对超时且提交状态不明的批次返回明确失败/不确定诊断，客户端重试可能重复，不假装已回滚。items错误信息不得泄露跨租户数据或凭据。

协议范围仅是摄入入口，不包含Elasticsearch mapping/template/ILM/license等探测接口。目标客户端若依赖这些接口，需单独实现有证据的必要子集或选择可配置输出器，不能伪造版本响应绕过检测。

## 6. 有界乱序、批处理和存储布局

第一阶段用到达微批做完整排序键检查/排序，不等待所有来源的全局watermark。每个tenant/dataset/shard桶按字节与最大等待时间flush；限制活跃桶总量，防止租户/来源高基数导致无限缓冲。保留背压，避免每个source独立生成大量小part。

先测event_time与ingest_time差、来源会话内逆序、晚到尾部与分片倾斜，再对比短重排窗口。来源空闲/断连不能阻塞全局进度。100TB/天入口1.157GB/s，10秒原始缓冲约11.57GB；分5个分片约2.31GB/分片，且不含解析/排序开销。不得预设60秒大内存重排。

稳定版Writer已有isAlreadySorted检查，有序块可跳过完整排序，但仍有校验/编码/索引/压缩等工作。需要以MergeTreeDataWriterBlocksAlreadySorted等指标及CPU profile验证，而非声称新增能力。

逻辑历史窗口与物理分区分开。先以按天分区为候选，比较两个排序布局：A=(tenant_id,service,event_time,event_id)，B=(time_bucket,tenant_id,service,event_time,event_id)。B的桶宽和查询约束必须一起测；不是仅有时间顺序就满足整个key。必要时增加时间优先基线。选择规则是完整摄入+查询成本最小，而非预先保证一种布局最佳。

最初保留标准merge机制；不得默认SYSTEM STOP MERGES。晚到日志追加新part并纳入查询。历史稳定/封窗策略是后续实验，不声称标准MergeTree自动实现。TTL按明确保留策略配置，在测试数据验证整体过期与删除时效，不直接清理用户现有文件或服务数据。

先用普通MergeTree做功能测试、ReplicatedMergeTree验证HA配置。6/8/10数据节点拓扑只是待测候选，Keeper及网关共享资源算入最多10台的预算。无外部额外节点假设。压缩比5:1、两副本、10天、70%目标使用率约需571TB总盘容量；未含其他存储，磁盘未提供前不能做最终部署定型。

## 7. 查询接口与SLO

POST /v1/logs/search：强制租户授权，time_from/time_to、dataset、service/host/severity、trace_id、文本谓词、limit、cursor。服务端白名单列名/操作符，值参数化；不接受任意SQL字符串。cursor绑定查询、租户与稳定排序，并签名。并发写入/晚到日志下分页可能漂移，v1不承诺跨请求数据库快照；需要稳定历史分页时限定ingest_time上界并测试边界。

POST /v1/logs/aggregate：有限时间桶计数、按服务/级别分组；上限约束时间范围、桶数与分组数，错误明确返回。v1暂不开放JOIN、任意脚本、深OFFSET和无限结果。

查询类别：Q1精确trace/结构字段、Q2近期日志TopN、Q3选择性token文本、Q4跨天宽范围文本/聚合。先为所有类别测100QPS下端到端p50/p95/p99；<100ms是否要求所有类别、哪个百分位需要用户确认。若达不到，明确FAIL，不把Q4排除后称原目标通过。

优先完整排序键裁剪、`PREWHERE`、晚读取message；text索引只对实际语义需要的字段启用，明确大小写/分词/中文/子串/正则差异。投影、Bloom、text索引都记录写入和merge成本，不能叠满所有索引。

数据可见延迟单列SLO，不与查询执行耗时混淆。查询超时/超预算返回清晰错误，不默默截断聚合得到错误结果。

## 8. 故障和安全边界

TLS、凭据授权、请求/解压后大小上限、参数化SQL、租户隔离必须从v0.1纳入。秘密仅存.env或受控secret挂载；提供无值.env.example，在版本管理前检查.env及变体已忽略，不提交真实日志、token或payload审计内容。

下游慢/不可用时限制内存、连接、排队和活跃批次，并用协议对应的重试错误施加背压；排队不能无限增长。无效日志拒绝或写入有配额的诊断隔离区，不使用无上限DLQ。首版不默认静默丢弃。

分别验证网关重启、请求超时后提交、部分分片故障、重复重试、乱序、时钟异常、磁盘满与权限错误。复制确认与断电持久性必须结合ClickHouse实际配置及故障测试描述，不能仅凭驱动返回成功推导所有故障都不丢数据。

## 9. 工作包与交付物

| 阶段 | 内容 | 交付与退出条件 |
|---|---|---|
| P0 契约与基准 | 数据样本、目标查询、乱序分析、节点/磁盘/网络、协议客户端清单 | OpenAPI、协议矩阵、schema候选、基准manifest；缺输入可用明确标记synthetic样本做功能，不当容量证据 |
| P1 可运行纵向切片 | Go服务、原生写入、HEC event、Bulk追加子集、规范化、批写、查询、单节点开发环境 | 端到端可写可查；成功/错误契约、认证、内存背压、重启和重试测试通过 |
| P2 兼容与可靠性 | HEC raw/ACK按需求、目标代理验证、复制配置与故障测试、可选持久队列评估 | 指定客户端版本可接入；失败/ACK/部分成功/隔离契约通过；不支持项明确 |
| P3 MergeTree实测 | 排序布局、输入格式、批大小、乱序窗口、codec、text索引、vertical merge与block大小 | 同硬件官方基线与逐项实验报告；parts/backlog稳定；不得输出未经验证收益 |
| P4 可选专用发行版 | 上游标签fork/镜像、裁剪清单、仅针对已证实热点的C++补丁 | upstream与fork A/B；功能/恢复回归；每个补丁有性能证据、升级说明和回滚 |
| P5 目标验收 | 100TB/天、10天规模、100QPS、明确延迟分位、故障/恢复/TTL | PASS/FAIL及完整资源成本；不满足也交付真实结果，不修改口径 |

P1拟议目录：cmd/logd、internal/ingest/{native,hec,bulk}、internal/normalize、internal/batch、internal/storage/clickhouse、internal/query、configs、deploy、tests/{contract,integration,failure}、bench、docs、todo。源代码尚未创建；不预估未经依据的开发工期。

P4候选补丁：内存感知merge准入、经profile证实的拷贝/分配热点、严格条件下非重叠part更便宜的合并。首先审计现有快速路径，若已存在则不重复实现；不保证直接复用压缩块。任何更改都保留排序正确性、索引一致性、校验与恢复语义。

## 10. 验收与证据

功能：每种入口可追踪到规范化字段及查询结果；无效时间、超限、协议错误、混合Bulk失败、跨租户访问、重试重复、HEC ACK状态、gzip边界均有必要契约/集成测试。正确性先于速度。

性能：固定版本、真实行宽/基数与查询、相同硬件和副本；记录原始字节吞吐、CPU秒/TB、压缩率、初始/merge写盘量、RSS/cgroup、parts、复制/merge积压、事件可见延迟和100QPS端到端尾延迟。协议适配器单独测，不能只拿Native基准代表HEC/Bulk。

稳态：覆盖正常合并、10天保留数据规模与TTL循环，并记录恢复追赶。短期加速测试可用于功能与趋势，但不能替代真实尺度的最终验收。100TB/天约1.157GB/s平均，峰值待提供；总资源仍限制最多10台，网关/Keeper/队列均入账。

状态：本计划、官方协议文档和已有稳定版源码核对已完成；P0用户数据/硬件输入待补充；P1–P5实现、构建、协议互操作、性能、故障验收全部NOT RUN。

## 11. 官方参考

- Elasticsearch Bulk契约：https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk
- Bulk中index覆盖与create冲突语义：https://www.elastic.co/guide/en/elasticsearch/reference/8.19/docs-bulk.html
- Splunk HEC端点：https://help.splunk.com/en/splunk-enterprise/rest-api-reference/10.4/input-endpoints/input-endpoint-descriptions
- Splunk HEC ACK及状态生命周期：https://help.splunk.com/en/splunk-enterprise/get-started/get-data-in/9.0/get-data-with-http-event-collector/about-http-event-collector-indexer-acknowledgment
- ClickHouse稳定版Writer快速路径：https://github.com/ClickHouse/ClickHouse/blob/2a96c27fd3bf3e455dfbd401f9cc241ce7327bd8/src/Storages/MergeTree/MergeTreeDataWriter.cpp#L882

协议文档用于设计依据，不代表本产品已通过原厂认证或全量兼容。
