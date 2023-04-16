package main

import (
	"fmt"

	"github.com/roseduan/minidb"
)

func main() {
	db, err := minidb.Open("/tmp/minidb")
	if err != nil {
		panic(err)
	}

	var (
		key   = []byte("dbname")
		value = []byte("minidb")
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
	fmt.Println("5. close minidb.")
}
