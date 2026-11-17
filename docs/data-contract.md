# sensorthings-api 来源契约

来源清单中的资产长度与 SHA-256 用于核对本地原文字节。文档的 collection 与 protocol 必须匹配清单，现有记录类型包括 thing, datastream, observation, sensor。每条记录以 kind、id、version 定位，refs 指向同集合的其他记录，payload 原样留存。来源版本冲突会中断接收；本地引用闭合只表示当前收到的引用能定位，不表示远端序列或授权资料已经完整。

## 已接收的协议资料
现有入口接收 SensorThings 1.1 风格的 @iot.id、phenomenonTime、Datastream 导航地址和 @iot.nextLink。只保存页关系，不把一页当作完整站点；跨主机续页及版本一致性尚未实现。


同集合第二次来源接收保存在 data/conflict-case.json。冲突事务必须保持第一份收据和旧投影。

## 受控站点快照采集

`snapshot-calls` 清单按序冻结一次受控采集的请求与响应（`calls`），`base_url` 为采集来源；可选 `token` 只附加到同源请求，跨源请求一律拒绝。采集器以 kind + @iot.id 为约定身份保存 Things、Datastreams、Observations 及关联的 Sensor、ObservedProperty；实体版本为规范化内容的 SHA-256。每条观测记录采集时刻所属 Datastream/Thing/Sensor/ObservedProperty 的版本，采集结束逐个核验实体版本。

覆盖证明：所有翻页流要求服务端排序（观测 phenomenonTime，其余 @iot.id）。分页令牌失效（410，或带分页参数的 400/404）时，从最后已见键以 ge 条件含边界重扫，区间可合并；只有取得无 nextLink 的末页，覆盖才闭合。页数本身不构成完整性证据。

显式状态：观测 ok / late（落入既往已证明区间）/ deleted（在已证明区间内消失，保留墓碑）/ id_conflict（同 ID 不同内容，多版本并存）；实体 ok / deleted。事件：token_expired、cross_origin_next_link（不跟随、凭据不外发）、entity_version_changed、entity_deleted、observation_deleted、late_observation、observation_restored、entity_restored。

运行闭合条件：三条流覆盖均闭合、无跨源 nextLink、无 id_conflict、采集期间实体版本未变、引用全部闭合。未闭合时原始观测仍可查询，但不组装站点视图，避免伪完整站点。
