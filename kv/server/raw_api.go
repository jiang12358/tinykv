package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	response := kvrpcpb.RawGetResponse {}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		response.Error = "ERROR"
		return &response, err
	}
	result, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		response.Error = "ERROR"
		return &response, err
	}

	if result == nil {
		response.Value = nil
		response.NotFound = true
	} else {
		response.Value = result
		response.NotFound = false
	}
	return &response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	response := kvrpcpb.RawPutResponse {}
	var modify []storage.Modify

	modify = append(modify, storage.Modify{
		Data: storage.Put{
			Key: req.Key,
			Value: req.Value,
			Cf: req.Cf,
		},
	})

	err := server.storage.Write(req.Context, modify)

	if err != nil {
		response.Error = "ERROR"
		return &response, err
	}

	return &response, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	response := kvrpcpb.RawDeleteResponse {}
	var modify []storage.Modify

	modify = append(modify, storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf: req.Cf,
		},
	})

	err := server.storage.Write(req.Context, modify)

	if err != nil {
		response.Error = "ERROR"
		return &response, err
	}

	return &response, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	response := kvrpcpb.RawScanResponse {}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		response.Error = "ERROR"
		return &response, err
	}

	iter := reader.IterCF(req.Cf)

	iter.Seek(req.StartKey)

	for i := uint32(0); i < req.Limit; i++ {
		if iter.Valid() {
			value, err := iter.Item().Value()
			if err != nil {
				response.Kvs = append(response.Kvs, &kvrpcpb.KvPair{
					Key: iter.Item().Key(),
					Error: &kvrpcpb.KeyError{},
				})
			} else {
				response.Kvs = append(response.Kvs, &kvrpcpb.KvPair{
					Key:   iter.Item().Key(),
					Value: value,
				})
			}
			iter.Next()
		} else {
			break
		}
	}

	return &response, nil
}
