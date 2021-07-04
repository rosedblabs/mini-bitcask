# minidb
rosedb 的 mini 版本，帮助理解 bitcask 存储模型以及 rosedb 项目。

需要说明的是，minidb 没有实现  bitcask 模型的多个数据文件的机制，为了简单，我只使用了一个数据文件进行读写。但这并不妨碍你理解 bitcask 模型。

我写了一篇文章对 minidb 进行讲解：[从零实现一个 k-v 存储引擎](https://mp.weixin.qq.com/s/s8s6VtqwdyjthR6EtuhnUA)

相信结合文章及 minidb 的简单的代码，你能够快速上手了。

当然，你可以阅读 bitcask 模型的论文原文：[https://riak.com/assets/bitcask-intro.pdf](https://riak.com/assets/bitcask-intro.pdf)

以及 rosedb 项目：[rosedb](https://github.com/roseduan/rosedb)

