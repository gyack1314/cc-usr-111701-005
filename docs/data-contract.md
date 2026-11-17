# sensorthings-api 来源契约

来源清单中的资产长度与 SHA-256 用于核对本地原文字节。文档的 collection 与 protocol 必须匹配清单，现有记录类型包括 thing, datastream, observation, sensor。每条记录以 kind、id、version 定位，refs 指向同集合的其他记录，payload 原样留存。来源版本冲突会中断接收；本地引用闭合只表示当前收到的引用能定位，不表示远端序列或授权资料已经完整。

## 已接收的协议资料
现有入口接收 SensorThings 1.1 风格的 @iot.id、phenomenonTime、Datastream 导航地址和 @iot.nextLink。只保存页关系，不把一页当作完整站点；跨主机续页及版本一致性尚未实现。


同集合第二次来源接收保存在 data/conflict-case.json。冲突事务必须保持第一份收据和旧投影。

同集合第二次来源接收保存在 data/conflict-case.json。冲突事务必须保持第一份收据和旧投影。
