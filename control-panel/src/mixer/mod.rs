//! 负责访问控制、执行策略并从 Envoy 代理中收集遥测数据。
//! Mixer 支持灵活的插件模型，方便扩展（支持 GCP、AWS、Prometheus、Heapster 等多种后端）