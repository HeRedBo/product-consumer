# product-consumer

## 项目介绍
海量数据高并发场景，构建Go+ES8企业级搜索微服务 产品同步队列消耗 服务代码

product-consumer作为为商品消费微服务，使用go语言开发。
作为shop-main商城消费者，商品数据变更后，shop-main将商品变更信息入kafka,product-consumer微服务作为消费端，从kafka中消费商品信息后写入ES
商城服务项目地址：https://github.com/HeRedBo/shop-main 

## 技术栈

1. elasticsearch
2. kafka
3. mongoDB 
4. logger 日志库
3. ~~promethues~~

## 运行方式
1. 项目根目录下执行 go run main.go
2. 项目根目录下执行 go build main.go ,直接运行生成的main程序

商品数据变更处理流程如下：
![商品搜索架构图](./doc/img/商品搜索架构图.png)
