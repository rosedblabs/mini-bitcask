package minidb

import (
	"io"
	"os"
	"path/filepath"
	"sync"
)

type MiniDB struct {
	indexes map[string]int64 // 内存中的索引信息
	dbFile  *DBFile          // 数据文件
	dirPath string           // 数据目录
	mu      sync.RWMutex
}

// Open 开启一个数据库实例
func Open(dirPath string) (*MiniDB, error) {
	// 如果数据库目录不存在，则新建一个
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 加载数据文件
	dbFile, err := NewDBFile(dirPath)
	if err != nil {
		return nil, err
	}

	db := &MiniDB{
		dbFile:  dbFile,
		indexes: make(map[string]int64),
		dirPath: dirPath,
	}

	// 加载索引
	db.loadIndexesFromFile(dbFile)
	return db, nil
}

// Merge 合并数据文件，在rosedb当中是 Reclaim 方法
func (db *MiniDB) Merge() error {
	// 没有数据，忽略
	if db.dbFile.Offset == 0 {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	var (
		validEntries []*Entry
		offset       int64
	)

	// 读取原数据文件中的 Entry
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 内存中的索引状态是最新的，直接对比过滤出有效的 Entry
		if off, ok := db.indexes[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()
	}

	if len(validEntries) > 0 {
		// 新建临时文件
		mergeDBFile, err := NewMergeDBFile(db.dirPath)
		if err != nil {
			return err
		}

		// 重新写入有效的 entry
		for _, entry := range validEntries {
			writeOff := mergeDBFile.Offset
			err := mergeDBFile.Write(entry)
			if err != nil {
				return err
			}

			// 更新索引
			db.indexes[string(entry.Key)] = writeOff
		}
		err = db.changeDBFile(mergeDBFile)
		if err != nil {
			return err
		}
	}
	return nil
}

// Put 写入数据
func (db *MiniDB) Put(key []byte, value []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// 从内存当中取出索引信息
	_, ok := db.indexes[string(key)]
	// key 存在则不添加
	if ok {
		return
	}

	offset := db.dbFile.Offset
	// 封装成 Entry
	entry := NewEntry(key, value, PUT)
	// 追加到数据文件当中
	err = db.dbFile.Write(entry)

	// 写到内存
	db.indexes[string(key)] = offset
	return
}

func (db *MiniDB) Keys() (keys []string) {
	for key := range db.indexes {
		keys = append(keys, key)
	}
	return keys
}

// Get 取出数据
func (db *MiniDB) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	// 从内存当中取出索引信息
	offset, ok := db.indexes[string(key)]
	// key 不存在
	if !ok {
		return
	}

	// 从磁盘中读取数据
	var e *Entry
	e, err = db.dbFile.Read(offset)
	if err != nil && err != io.EOF {
		return
	}
	if e != nil {
		val = e.Value
	}
	return
}

// Del 删除数据
func (db *MiniDB) Del(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	// 从内存当中取出索引信息
	_, ok := db.indexes[string(key)]
	// key 不存在，忽略
	if !ok {
		return
	}

	// 封装成 Entry 并写入
	e := NewEntry(key, nil, DEL)
	err = db.dbFile.Write(e)
	if err != nil {
		return
	}

	// 删除内存中的 key
	delete(db.indexes, string(key))
	return
}

// 从文件当中加载索引
func (db *MiniDB) loadIndexesFromFile(dbFile *DBFile) {
	if dbFile == nil {
		return
	}

	var offset int64
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			// 读取完毕
			if err == io.EOF {
				break
			}
			return
		}
		key := string(e.Key)
		if e.Mark == PUT {
			// 设置索引状态
			db.indexes[key] = offset
		}
		if e.Mark == DEL {
			delete(db.indexes, key)
		}
		offset += e.GetSize()
	}
	return
}

func (db *MiniDB) changeDBFile(dbFile *DBFile) (err error) {
	err = db.dbFile.File.Close()
	if err != nil {
		return err
	}
	err = os.Remove(db.dbFile.File.Name())
	if err != nil {
		return err
	}
	err = dbFile.File.Close()
	if err != nil {
		return err
	}
	err = os.Rename(dbFile.File.Name(), filepath.Join(db.dirPath, FileName))
	if err != nil {
		return err
	}
	db.dbFile, err = NewDBFile(db.dirPath)
	return err
}
