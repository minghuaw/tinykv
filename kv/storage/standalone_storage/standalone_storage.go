package standalone_storage

import (
	"errors"

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
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	if s != nil {
		err := errors.New("StandAloneStorage does not exist")
		return nil, err
	}
	return s, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		cf := modify.Cf()
		key := modify.Key()
		val := modify.Value()
		err := engine_util.PutCF(s.db, cf, key, val)

		if err != nil {
			return err
		}
	}
	return nil
}

func (s *StandAloneStorage) GetCF(cf string, key []byte) ([]byte, error) {
	// set update to false for read-only transaction
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	return engine_util.GetCFFromTxn(txn, cf, key)
}

func (s *StandAloneStorage) IterCF(cf string) engine_util.DBIterator {
	// set update to false for read-only transaction
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	return engine_util.NewCFIterator(cf, txn)
}

func (s *StandAloneStorage) Close() {
	s.db.Close()
}
