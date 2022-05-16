package commands

import (
	"encoding/hex"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Get struct {
	ReadOnly
	CommandBase
	request *kvrpcpb.GetRequest
}

func NewGet(request *kvrpcpb.GetRequest) Get {
	return Get{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.Version,
		},
		request: request,
	}
}

//goland:noinspection ALL
func (g *Get) Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error) {
	key := g.request.Key
	log.Debug("read key", zap.Uint64("start_ts", txn.StartTS),
		zap.String("key", hex.EncodeToString(key)))
	response := new(kvrpcpb.GetResponse)

	// YOUR CODE HERE (lab1).
	// Check for locks and their visibilities.
	// Hint: Check the interfaces provided by `mvcc.RoTxn`.

	// if there is a lock on key
	lock, err := txn.GetLock(key)

	if lock != nil && lock.Kind == mvcc.WriteKindPut {

		if g.request.Version < lock.Ts {
			value, _ := txn.GetValue(key)
			response.Value = value
			return response, nil, nil
		}

		lockInfo := kvrpcpb.LockInfo{PrimaryLock: lock.Primary, LockVersion: lock.Ts, Key: key, LockTtl: lock.Ttl}
		keyError := kvrpcpb.KeyError{Locked: &lockInfo}
		response.Error = &keyError
		return response, nil, err
	}

	// YOUR CODE HERE (lab1).
	// Search writes for a committed value, set results in the response.
	// Hint: Check the interfaces provided by `mvcc.RoTxn`.
	value, _ := txn.GetValue(key)

	if value == nil {
		log.Info("value 为空")
		response.Value = nil
		response.NotFound = true
		return response, nil, nil
	}

	response.Value = value
	response.Error = nil

	return response, nil, nil
}
