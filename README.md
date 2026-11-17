# OGC SensorThings 多实体翻页与观测引用采集资料服务

项目接收 sensorthings-api 来源资料，现有目录只核对 thing, datastream, observation, sensor 的基本身份和引用。`capture_data.io` 冻结字节和请求来源，`capture_data.domain` 核对本领域记录结构，`capture_data.journal` 封存来源原文和版本，`capture_data.projection` 显示本地引用缺口。现有判断只覆盖本地资料，尚未推断外部服务的连续性或全量完整性。

Python 3.11 以上，无第三方依赖。样例均为合成，不访问真实站点。

```sh
python -m unittest discover -s tests -v
python -m compileall -q capture_data tests
python -m capture_data.cli inspect data/normal.json
```

Dockerfile 提供 Linux 运行环境；`docker build -t source-capture .` 构建，`docker run --rm source-capture python -m unittest discover -s tests -v` 运行测试。

协议样例可通过 python -m capture_data.cli inspect data/protocol-case.json 查看。该命令仍只说明现有接收能力，不证明远端连续性。

## 受控站点快照采集

`capture_data.snapshot` 在冻结呼叫回放（或真实传输）上执行受控采集，样例均为合成：

```sh
python -m capture_data.cli snapshot data/snapshot-ok.json        # 闭合站点视图
python -m capture_data.cli snapshot data/snapshot-token.json     # 令牌失效后按可证明范围重扫
python -m capture_data.cli snapshot data/snapshot-foreign.json   # 跨源 nextLink 被拒绝，运行保持开放
```

快照按约定身份（kind + @iot.id + 内容版本）保存 Things、Datastreams、Observations 及关联 Sensor、ObservedProperty；每条观测保留其采集时刻的实体版本。跨源 nextLink 不跟随、凭据不外发；删除、晚到、同 ID 冲突都有显式状态；引用未闭合时只能查询原始观测，得不到伪完整站点。详见 docs/data-contract.md。
