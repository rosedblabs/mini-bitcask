# mini-bitcask

rosedb 的 mini 版本，帮助理解 bitcask 存储模型以及 rosedb 项目。

需要说明的是，mini-bitcask 没有实现 bitcask 模型的多个数据文件的机制，为了简单，我只使用了一个数据文件进行读写。但这并不妨碍你理解 bitcask 模型。

我写了一篇文章对 mini-bitcask 进行讲解：[从零实现一个 k-v 存储引擎](https://mp.weixin.qq.com/s/s8s6VtqwdyjthR6EtuhnUA)，相信结合文章及 mini-bitcask 的简单的代码，你能够快速上手了。
> 文章中叫 minidb，但是后来觉得不妥，现已更名为 mini-bitcask

## reference

### bitcask 模型的论文

[https://riak.com/assets/bitcask-intro.pdf](https://riak.com/assets/bitcask-intro.pdf)

### rosedb 项目

[rosedb](https://github.com/roseduan/rosedb)

## Usage

```go
package main

import (
	"fmt"

	"github.com/roseduan/minibitcask"
)

func main() {
	db, err := minibitcask.Open("/tmp/minibitcask")
	if err != nil {
		panic(err)
	}

	var (
		key   = []byte("dbname")
		value = []byte("minibitcask")
	)

	err = db.Put(key, value)
	if err != nil {
		panic(err)
	}
	fmt.Printf("1. put kv successfully, key: %s, value: %s.\n", string(key), string(value))

	cur, err := db.Get(key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("2. get value of key %s, the value of key %s is %s.\n", string(key), string(key), string(cur))

	err = db.Del(key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("3. delete key %s.\n", string(key))

	db.Merge()
	fmt.Println("4. compact data to new dbfile.")

	db.Close()
	fmt.Println("5. close minibitcask.")
}
```