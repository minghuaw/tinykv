package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	db := engine_util.CreateDB(conf.DBPath, false)
	standalone := StandAloneStorage{db: db}
	return &standalone
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	reader := BadgerTxnReader{txn: txn}
	return &reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var err error
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			cf := modify.Cf()
			key := modify.Key()
			val := modify.Value()
			err = engine_util.PutCF(s.db, cf, key, val)
		case storage.Delete:
			cf := modify.Cf()
			key := modify.Key()
			err = engine_util.DeleteCF(s.db, cf, key)
		}

		if err != nil {
			return err
		}
	}
	return nil
}

type BadgerTxnReader struct {
	txn *badger.Txn
}

func (b *BadgerTxnReader) GetCF(cf string, key []byte) ([]byte, error) {
	// set update to false for read-only transaction
	val, err := engine_util.GetCFFromTxn(b.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, nil
}

func (b *BadgerTxnReader) IterCF(cf string) engine_util.DBIterator {
	// set update to false for read-only transaction
	return engine_util.NewCFIterator(cf, b.txn)
}

func (b *BadgerTxnReader) Close() {
	b.txn.Discard()
}
