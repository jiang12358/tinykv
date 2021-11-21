package standalone_storage

import (
	"bytes"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorageReader(txn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		txn:    txn,
	}
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	var iter = engine_util.NewCFIterator(cf, r.txn)
	iter.Seek(key)
	if iter.Valid() && bytes.Equal(iter.Item().Key(), key) {
		return iter.Item().Value()
	}

	return nil, nil
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}

