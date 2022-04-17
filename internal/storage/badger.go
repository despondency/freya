package storage

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/pb"
	"github.com/francoispqt/gojay"
	"github.com/golang/protobuf/proto"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	appliedIndexKey    string = "disk_kv_applied_index"
	databaseDirName    string = "database"
	currentDBFilename  string = "current"
	updatingDBFilename string = "current.updating"
)

type StringSlice []string

func (t *StringSlice) MarshalJSONArray(enc *gojay.Encoder) {
	for _, e := range *t {
		enc.String(e)
	}
}

func (t *StringSlice) IsNil() bool {
	return t == nil
}

func (t *StringSlice) UnmarshalJSONArray(dec *gojay.Decoder) error {
	str := ""
	if err := dec.String(&str); err != nil {
		return err
	}
	*t = append(*t, str)
	return nil
}

type KVData struct {
	Keys   StringSlice
	Values StringSlice
}

func (kv *KVData) UnmarshalJSONObject(dec *gojay.Decoder, key string) error {
	switch key {
	case "k":
		return dec.Array(&kv.Keys)
	case "v":
		return dec.Array(&kv.Values)
	}
	return nil
}

func (kv *KVData) NKeys() int {
	return 2
}

func (kv *KVData) MarshalJSONObject(enc *gojay.Encoder) {
	enc.ArrayKey("k", &kv.Keys)
	enc.ArrayKey("v", &kv.Values)
}

func (kv *KVData) IsNil() bool {
	return kv == nil
}

type BadgerStorage struct {
	db            unsafe.Pointer
	clusterID     uint64
	nodeID        uint64
	lastApplied   uint64
	lastAppliedTs uint64
	closed        bool
	aborted       bool
	mu            *sync.RWMutex
}

func NewBadgerStorage(clusterID, nodeID uint64) sm.IOnDiskStateMachine {
	return &BadgerStorage{
		clusterID: clusterID,
		nodeID:    nodeID,
		mu:        &sync.RWMutex{},
	}
}

func (b *BadgerStorage) Open(stopc <-chan struct{}) (uint64, error) {
	dir := getCurrentNodeDBDirName(b.clusterID, b.nodeID)
	if err := createNodeDataDir(dir); err != nil {
		panic(err)
	}
	var databaseDir string
	if !isInitialRun(dir) {
		if err := cleanupNodeDataDir(dir); err != nil {
			return 0, err
		}
		var err error
		databaseDir, err = getCurrentDBDirName(dir)
		if err != nil {
			return 0, err
		}
		if _, err := os.Stat(databaseDir); err != nil {
			if os.IsNotExist(err) {
				panic("db dir unexpectedly deleted")
			}
		}
	} else {
		databaseDir = getNewRandomDBDirName(dir)
		if err := saveCurrentDBDirName(dir, databaseDir); err != nil {
			return 0, err
		}
		if err := replaceCurrentDBFile(dir); err != nil {
			return 0, err
		}
	}
	db, err := createDB(databaseDir)
	if err != nil {
		return 0, err
	}
	atomic.SwapPointer(&b.db, unsafe.Pointer(db))
	appliedIndex, err := b.queryAppliedIndex(db)
	if err != nil {
		panic(err)
	}
	b.lastApplied = appliedIndex
	return appliedIndex, nil
}

func (b *BadgerStorage) queryAppliedIndex(db *badger.DB) (uint64, error) {
	var val []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(appliedIndexKey))
		if err != nil {
			return err
		}
		val = make([]byte, item.ValueSize())
		_, err = item.ValueCopy(val)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil && err != badger.ErrKeyNotFound {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(val), nil
}

func createDB(databaseDir string) (*badger.DB, error) {
	opts := badger.DefaultOptions(databaseDir)
	return badger.Open(opts)
}

func replaceCurrentDBFile(dir string) error {
	fp := filepath.Join(dir, currentDBFilename)
	tmpFp := filepath.Join(dir, updatingDBFilename)
	if err := os.Rename(tmpFp, fp); err != nil {
		return err
	}
	return syncDir(dir)
}

func saveCurrentDBDirName(dir string, dbdir string) error {
	h := md5.New()
	if _, err := h.Write([]byte(dbdir)); err != nil {
		return err
	}
	fp := filepath.Join(dir, updatingDBFilename)
	f, err := os.Create(fp)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
		if err := syncDir(dir); err != nil {
			panic(err)
		}
	}()
	if _, err := f.Write(h.Sum(nil)[:8]); err != nil {
		return err
	}
	if _, err := f.Write([]byte(dbdir)); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

func getNewRandomDBDirName(dir string) string {
	part := "%d_%d"
	rn := rand.Uint64()
	ct := time.Now().UnixNano()
	return filepath.Join(dir, fmt.Sprintf(part, rn, ct))
}

func getCurrentDBDirName(dir string) (string, error) {
	fp := filepath.Join(dir, currentDBFilename)
	f, err := os.OpenFile(fp, os.O_RDONLY, 0755)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return "", err
	}
	if len(data) <= 8 {
		panic("corrupted content")
	}
	crc := data[:8]
	content := data[8:]
	h := md5.New()
	if _, err := h.Write(content); err != nil {
		return "", err
	}
	if !bytes.Equal(crc, h.Sum(nil)[:8]) {
		panic("corrupted content with not matched crc")
	}
	return string(content), nil
}

func cleanupNodeDataDir(dir string) error {
	os.RemoveAll(filepath.Join(dir, updatingDBFilename))
	dbdir, err := getCurrentDBDirName(dir)
	if err != nil {
		return err
	}
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, fi := range files {
		if !fi.IsDir() {
			continue
		}
		fmt.Printf("dbdir %s, fi.name %s, dir %s\n", dbdir, fi.Name(), dir)
		toDelete := filepath.Join(dir, fi.Name())
		if toDelete != dbdir {
			fmt.Printf("removing %s\n", toDelete)
			if err := os.RemoveAll(toDelete); err != nil {
				return err
			}
		}
	}
	return nil
}

func isInitialRun(dir string) bool {
	fp := filepath.Join(dir, currentDBFilename)
	if _, err := os.Stat(fp); os.IsNotExist(err) {
		return true
	}
	return false
}

func syncDir(dir string) (err error) {
	if runtime.GOOS == "windows" {
		return nil
	}
	fileInfo, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !fileInfo.IsDir() {
		panic("not a dir")
	}
	df, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}
	defer func() {
		if cerr := df.Close(); err == nil {
			err = cerr
		}
	}()
	return df.Sync()
}

func createNodeDataDir(dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dir))
}

func getCurrentNodeDBDirName(clusterID uint64, nodeID uint64) string {
	part := fmt.Sprintf("%d_%d", clusterID, nodeID)
	return filepath.Join(databaseDirName, part)
}

func (b *BadgerStorage) Update(entries []sm.Entry) ([]sm.Entry, error) {
	if b.aborted {
		panic("update() called after abort set to true")
	}
	if b.closed {
		panic("update called after Close()")
	}
	db := (*badger.DB)(atomic.LoadPointer(&b.db))
	wb := db.NewWriteBatch()
	for idx, e := range entries {
		dataKV := &KVData{}
		if err := gojay.UnmarshalJSONObject(e.Cmd, dataKV); err != nil {
			panic(err)
		}
		for i, k := range dataKV.Keys {
			err := wb.Set([]byte(k), []byte(dataKV.Values[i]))
			if err != nil {
				panic(err)
			}
		}
		entries[idx].Result = sm.Result{Value: uint64(len(entries[idx].Cmd))}
	}
	// save the applied index to the DB.
	appliedIndex := make([]byte, 8)
	binary.LittleEndian.PutUint64(appliedIndex, entries[len(entries)-1].Index)
	err := wb.Set([]byte(appliedIndexKey), appliedIndex)
	if err != nil {
		panic(err)
	}
	if err = wb.Flush(); err != nil {
		return nil, err
	}
	if b.lastApplied >= entries[len(entries)-1].Index {
		panic("lastApplied not moving forward")
	}
	b.lastApplied = entries[len(entries)-1].Index
	return entries, nil
}

func (b *BadgerStorage) Lookup(key interface{}) (interface{}, error) {
	db := (*badger.DB)(atomic.LoadPointer(&b.db))
	if db != nil {
		v, err := b.lookup(db, key.([]byte))
		if err == nil && b.closed {
			panic("closed while returning correct answer")
		}
		if err == badger.ErrKeyNotFound {
			return v, nil
		}
		return v, err
	}
	return nil, errors.New("db closed")
}

func (b *BadgerStorage) lookup(db *badger.DB, key []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, errors.New("db already closed")
	}
	var val []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val = make([]byte, item.ValueSize())
		_, err = item.ValueCopy(val)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, nil
	}
	return val, nil
}

func (b *BadgerStorage) Sync() error {
	db := (*badger.DB)(atomic.LoadPointer(&b.db))
	return db.Sync()
}

type badgerBackup struct {
	backup *bytes.Buffer
	db     *badger.DB
}

func (b *BadgerStorage) PrepareSnapshot() (interface{}, error) {
	if b.closed {
		panic("prepare snapshot called after Close()")
	}
	if b.aborted {
		panic("prepare snapshot called after abort")
	}
	db := (*badger.DB)(atomic.LoadPointer(&b.db))
	buff := make([]byte, 0)
	bytesBuffer := bytes.NewBuffer(buff)
	_, err := db.Backup(bytesBuffer, b.lastAppliedTs)
	if err != nil {
		return nil, err
	}
	return &badgerBackup{
		backup: bytesBuffer,
	}, nil
}

func (b *BadgerStorage) SaveSnapshot(ctx interface{}, writer io.Writer, i2 <-chan struct{}) error {
	if b.closed {
		panic("prepare snapshot called after Close()")
	}
	if b.aborted {
		panic("prepare snapshot called after abort")
	}
	ctxdata := ctx.(*badgerBackup)
	b.mu.Lock()
	defer b.mu.Unlock()
	kvList := &pb.KVList{}
	err := proto.Unmarshal(ctxdata.backup.Bytes(), kvList)
	if err != nil {
		panic(err)
	}
	return b.saveToWriter(kvList, writer)
}

func (b *BadgerStorage) saveToWriter(kvList *pb.KVList, w io.Writer) error {
	values := make([]*KVData, 0)
	for _, kv := range kvList.Kv {
		kvData := &KVData{
			Keys:   StringSlice{string(kv.Value)},
			Values: StringSlice{string(kv.Key)},
		}
		values = append(values, kvData)
	}
	count := uint64(len(values))
	sz := make([]byte, 8)
	binary.LittleEndian.PutUint64(sz, count)
	if _, err := w.Write(sz); err != nil {
		return err
	}
	for _, dataKv := range values {
		data, err := gojay.MarshalJSONObject(dataKv)
		if err != nil {
			panic(err)
		}
		binary.LittleEndian.PutUint64(sz, uint64(len(data)))
		if _, err := w.Write(sz); err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return nil
}

func (b *BadgerStorage) RecoverFromSnapshot(reader io.Reader, i <-chan struct{}) error {
	if b.closed {
		panic("recover from snapshot called after Close()")
	}
	dir := getCurrentNodeDBDirName(b.clusterID, b.nodeID)
	dbdir := getNewRandomDBDirName(dir)
	oldDirName, err := getCurrentDBDirName(dir)
	if err != nil {
		return err
	}
	db, err := createDB(dbdir)
	if err != nil {
		return err
	}
	sz := make([]byte, 8)
	if _, err := io.ReadFull(reader, sz); err != nil {
		return err
	}
	total := binary.LittleEndian.Uint64(sz)
	wb := db.NewWriteBatch()
	for i := uint64(0); i < total; i++ {
		if _, err := io.ReadFull(reader, sz); err != nil {
			return err
		}
		toRead := binary.LittleEndian.Uint64(sz)
		data := make([]byte, toRead)
		if _, err := io.ReadFull(reader, data); err != nil {
			return err
		}
		dataKv := &KVData{}
		if err := gojay.UnmarshalJSONObject(data, dataKv); err != nil {
			panic(err)
		}
		if len(dataKv.Values) != len(dataKv.Keys) {
			panic("Keys and Values need to have same length, corrupted?")
		}
		for j, k := range dataKv.Keys {
			err = wb.Set([]byte(k), []byte(dataKv.Values[j]))
			if err != nil {
				panic(err)
			}
		}
	}
	if err := wb.Flush(); err != nil {
		return err
	}
	if err := db.Sync(); err != nil {
		return err
	}
	if err := saveCurrentDBDirName(dir, dbdir); err != nil {
		return err
	}
	if err := replaceCurrentDBFile(dir); err != nil {
		return err
	}
	newLastApplied, err := b.queryAppliedIndex(db)
	if err != nil {
		panic(err)
	}
	// when d.lastApplied == newLastApplied, it probably means there were some
	// dummy entries or membership change entries as part of the new snapshot
	// that never reached the SM and thus never moved the last applied index
	// in the SM snapshot.
	if b.lastApplied > newLastApplied {
		panic("last applied not moving forward")
	}
	b.lastApplied = newLastApplied
	old := (*badger.DB)(atomic.SwapPointer(&b.db, unsafe.Pointer(db)))
	if old != nil {
		old.Close()
	}
	parent := filepath.Dir(oldDirName)
	if err := os.RemoveAll(oldDirName); err != nil {
		return err
	}
	return syncDir(parent)
}

func (b *BadgerStorage) Close() error {
	db := (*badger.DB)(atomic.SwapPointer(&b.db, unsafe.Pointer(nil)))
	if db != nil {
		b.closed = true
		db.Close()
	} else {
		if b.closed {
			panic("close called twice")
		}
	}
	return nil
}
