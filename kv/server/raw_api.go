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
	s, err1 := server.storage.Reader(nil)
	defer s.Close()
	t := &kvrpcpb.RawGetResponse{}
	if err1 != nil {
		t.Error = err1.Error()
		return t, err1
	}
	val, err2 := s.GetCF(req.GetCf(), req.GetKey())
	if err2 != nil {
		t.Error = err2.Error()
		return t, err2
	}
	if val == nil {
		t.NotFound = true
	}
	t.Value = val
	return t, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	t := &kvrpcpb.RawPutResponse{}
	q := make([]storage.Modify, 1)
	q[0].Data = storage.Put{
		Cf:    req.GetCf(),
		Key:   req.GetKey(),
		Value: req.GetValue(),
	}
	err := server.storage.Write(nil, q)
	if err != nil {
		t.Error = err.Error()
		return t, err
	}
	return t, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	t := &kvrpcpb.RawDeleteResponse{}
	q := make([]storage.Modify, 1)
	q[0].Data = storage.Delete{
		Cf:  req.GetCf(),
		Key: req.GetKey(),
	}
	err := server.storage.Write(nil, q)
	if err != nil {
		t.Error = err.Error()
		return t, err
	}
	return t, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	t := &kvrpcpb.RawScanResponse{}
	s, err1 := server.storage.Reader(nil)
	defer s.Close()
	if err1 != nil {
		t.Error = err1.Error()
		return t, err1
	}
	iter := s.IterCF(req.GetCf())
	defer iter.Close()
	iter.Seek(req.GetStartKey())
	for i := uint32(0); i < req.GetLimit() && iter.Valid(); i++ {
		item := iter.Item()
		val, err2 := item.Value()
		if err2 != nil {
			t.Error = err2.Error()
			return t, err2
		}
		t.Kvs = append(t.Kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		})
		iter.Next()
	}
	return t, nil
}
