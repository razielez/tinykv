package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (s StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	return val, err
}

func (s StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s StandAloneStorageReader) Close() {
	s.txn.Discard()
}

func NewStandAloneStorageReader(txn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		txn: txn,
	}
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	log.Infof("db path: ", conf.DBPath)
	kvPath := filepath.Join(conf.DBPath, "StandAloneStorage", "kv")
	// if file not exist create dir
	if _, err := os.Stat(kvPath); err != nil {
		err := os.MkdirAll(kvPath, os.ModePerm)
		if err != nil {
			log.Errorf("create dir error: %v", err)
			return nil
		}
		db := engine_util.CreateDB(kvPath, false)
		log.Infof("create db done!")
		return &StandAloneStorage{
			engines: engine_util.NewEngines(db, nil, kvPath, ""),
		}
	}
	return nil
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.engines.Kv.Close()
	if err != nil {
		log.Errorf("close db error: %v", err)
		return err
	}
	if err := os.RemoveAll(s.engines.KvPath); err != nil {
		log.Error(err)
		return err
	}
	log.Infof("remove db path: %v", s.engines.KvPath)
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneStorageReader(s.engines.Kv.NewTransaction(false)), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	b := new(engine_util.WriteBatch)
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			b.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			b.DeleteCF(data.Cf, data.Key)
		}
	}
	return s.engines.WriteKV(b)
}
