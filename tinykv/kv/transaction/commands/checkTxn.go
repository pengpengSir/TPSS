package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type CheckTxnStatus struct {
	CommandBase
	request *kvrpcpb.CheckTxnStatusRequest
}

func NewCheckTxnStatus(request *kvrpcpb.CheckTxnStatusRequest) CheckTxnStatus {
	return CheckTxnStatus{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.LockTs,
		},
		request: request,
	}
}

func (c *CheckTxnStatus) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	key := c.request.PrimaryKey
	response := new(kvrpcpb.CheckTxnStatusResponse)

	lock, err := txn.GetLock(key) // 这已经是pk的lock

	if err != nil {
		return nil, err
	}

	if lock != nil && lock.Ts == txn.StartTS {
		if physical(lock.Ts)+lock.Ttl < physical(c.request.CurrentTs) {
			// YOUR CODE HERE (lab1).
			// Lock has expired, try to rollback it. `mvcc.WriteKindRollback` could be used to
			// represent the type. Try using the interfaces provided by `mvcc.MvccTxn`.
			// 应该先rollback再删除锁
			txn.PutWrite(key, lock.Ts, &(mvcc.Write{txn.StartTS, mvcc.WriteKindRollback})) // 第二个是commitTs，如果rollback需要将commitTs设为0
			txn.DeleteLock(key)
			txn.DeleteValue(key) // 和txn.PutWrite(WriteKindRollback)是不是重复了

			response.Action = kvrpcpb.Action_TTLExpireRollback
			response.CommitVersion = 0
			response.LockTtl = 0

			log.Info("checkTxnStatus rollback the primary lock as it's expired",
				zap.Uint64("lock.TS", lock.Ts),
				zap.Uint64("physical(lock.TS)", physical(lock.Ts)),
				zap.Uint64("txn.StartTS", txn.StartTS),
				zap.Uint64("currentTS", c.request.CurrentTs),
				zap.Uint64("physical(currentTS)", physical(c.request.CurrentTs)))
		} else {
			// Lock has not expired, leave it alone.
			response.Action = kvrpcpb.Action_NoAction
			response.LockTtl = lock.Ttl
		}

		return response, nil
	}

	existingWrite, commitTs, err := txn.CurrentWrite(key)
	if err != nil {
		return nil, err
	}
	if existingWrite == nil {
		// YOUR CODE HERE (lab1).
		// The lock never existed, it's still needed to put a rollback record on it so that
		// the stale transaction commands such as prewrite on the key will fail.
		// Note try to set correct `response.Action`,
		// the action types could be found in kvrpcpb.Action_xxx.

		txn.PutWrite(key, txn.StartTS, &(mvcc.Write{txn.StartTS, mvcc.WriteKindRollback}))
		response.Action = kvrpcpb.Action_LockNotExistRollback
		response.LockTtl = 0
		return response, nil
	}

	if existingWrite.Kind == mvcc.WriteKindRollback {
		// The key has already been rolled back, so nothing to do.
		response.Action = kvrpcpb.Action_NoAction
		return response, nil
	}

	// The key has already been committed.
	response.CommitVersion = commitTs
	response.Action = kvrpcpb.Action_NoAction
	return response, nil
}

func physical(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}

func (c *CheckTxnStatus) WillWrite() [][]byte {
	return [][]byte{c.request.PrimaryKey}
}
