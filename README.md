## 支持kafka多版本

### 问题
1. 由于kafka client对kafka版本的兼容性做的不是很好，想针对不同版本的server，加载不同版本的client解决这个问题
2. 系统里需要访问多个kafka集群，并且kafka的版本有可能不一致，想同时运行不同版本的kafka client来同时对接多个server

针对以上两个痛点，本工具应运而生

### 解决方案 
1. 使用socket直连kafka，通过api version来嗅探kafka的版本，然后加载不同的kafka版本的client来处理
2. 使用classloader来做不同版本的代码隔离
3. 通过proxy来代理访问不同版本的kafka，具体的proxy使用groovy脚本实现

