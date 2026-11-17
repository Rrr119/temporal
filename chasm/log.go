package chasm

import (
	"context"
	"errors"
	"iter"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

const (
	logNodeFieldName                 = "node"
	logMetadataFieldName             = "metadata"
	logNewHeadBatchIDFieldName       = "newHeadBatchID"
	logNewBatchesFieldName           = "newBatches"
	logMessageBatchMessagesFieldName = "messages"
)

var ErrInvalidBatchID = errors.New("invalid batch ID")

type logMessageBatch[T proto.Message] struct {
	batchID  int64
	messages []proto.Message
}

type Log[T proto.Message] struct {
	metadata *persistencespb.ChasmLogMetadata
	node     *Node

	newHeadBatchID int64
	newBatches     []*logMessageBatch[T]
}

type LogMessage[T proto.Message] struct {
	sequenceID int64
	message    proto.Message
}

func (lm *LogMessage[T]) SequenceID() int64 {
	return lm.sequenceID
}

func (lm *LogMessage[T]) Message() T {
	return lm.message.(T)
}

func NewLogMessage[T proto.Message](sequenceID int64, message T) *LogMessage[T] {
	return &LogMessage[T]{
		sequenceID: sequenceID,
		message:    message,
	}
}

func (log *Log[T]) Append(messages ...T) {
	if len(messages) == 0 {
		return
	}
	batch := &logMessageBatch[T]{
		messages: make([]proto.Message, len(messages)),
	}
	if len(log.newBatches) > 0 {
		lastBatch := log.newBatches[len(log.newBatches)-1]
		batch.batchID = lastBatch.batchID + int64(len(lastBatch.messages))
	} else {
		batch.batchID = log.metadata.GetLastSequenceId() + 1
	}
	for i, message := range messages {
		batch.messages[i] = message
	}
	log.newBatches = append(log.newBatches, batch)
}

func (log *Log[T]) AdvanceHead(ctx context.Context, batchID int64) error {
	if batchID <= log.metadata.GetLastSequenceId() {
		token, err := proto.Marshal(&tokenspb.HistoryEventRef{
			EventBatchId: batchID,
		})
		if err != nil {
			return err
		}
		batch, err := log.node.backend.LoadChasmLogMessages(ctx, token, log.metadata.GetBranch())
		if err != nil {
			return err
		}
		if batch.Messages[0].SequenceId != batchID {
			return ErrInvalidBatchID
		}
	} else {
		lastSequenceID := log.metadata.GetLastSequenceId()
		idx := 0
		for idx < len(log.newBatches) && log.newBatches[idx].batchID < batchID {
			lastSequenceID = log.newBatches[idx].batchID + int64(len(log.newBatches[idx].messages)) - 1
			idx++
		}
		if lastSequenceID+1 != batchID {
			return ErrInvalidBatchID
		}
		log.newBatches = log.newBatches[idx:]
	}
	log.newHeadBatchID = batchID
	return nil
}

// Read returns an iterator to the message at or after offset.
func (log *Log[T]) Read(ctx context.Context, offset int64) *LogMessageIterator[T] {
	batchID := max(common.FirstEventID, log.metadata.GetFirstSequenceId()+offset)
	return newLogMessageIterator(ctx, log, batchID)
}

// ReadBatch returns an iterator to the batch of messages at or after offset.
func (log *Log[T]) ReadBatch(ctx context.Context, offset int64) *LogMessageBatchIterator[T] {
	batchID := max(common.FirstEventID, log.metadata.GetFirstSequenceId()+offset)
	return newLogMessageBatchIterator(ctx, log, batchID)
}

type LogMessageBatchIterator[T proto.Message] struct {
	ctx context.Context
	log *Log[T]

	token             *tokenspb.HistoryEventRef
	currBatch         []*persistencespb.ChasmLogMessage
	currNewBatchesIdx int
}

func newLogMessageBatchIterator[T proto.Message](
	ctx context.Context,
	log *Log[T],
	batchID int64,
) *LogMessageBatchIterator[T] {
	return &LogMessageBatchIterator[T]{
		ctx: ctx,
		log: log,

		token: &tokenspb.HistoryEventRef{
			EventBatchId: batchID,
		},
	}
}

func (it *LogMessageBatchIterator[T]) HasNext() bool {
	if it.ctx.Err() != nil {
		return false
	}
	if it.currBatch != nil {
		return true
	}
	if it.token == nil {
		return false
	}
	if it.token.EventBatchId <= it.log.metadata.GetLastSequenceId() {
		token, err := proto.Marshal(it.token)
		if err != nil {
			return false
		}
		batch, err := it.log.node.backend.LoadChasmLogMessages(it.ctx, token, it.log.metadata.GetBranch())
		if err != nil {
			return false
		}
		if len(batch.GetMessages()) == 0 {
			return false
		}
		it.currBatch = batch.Messages
		it.token.EventBatchId = batch.Messages[len(batch.Messages)-1].SequenceId + 1
		return true
	}
	for it.currNewBatchesIdx < len(it.log.newBatches) &&
		it.log.newBatches[it.currNewBatchesIdx].batchID < it.token.EventBatchId {
		it.currNewBatchesIdx++
	}
	return it.currNewBatchesIdx < len(it.log.newBatches)
}

func (it *LogMessageBatchIterator[T]) Next() ([]*LogMessage[T], error) {
	batch := make([]*LogMessage[T], len(it.currBatch))
	i := 0
	for message, err := range it.batchMessageIter() {
		if err != nil {
			return nil, err
		}
		batch[i] = message
		i++
	}
	return batch, nil
}

func (it *LogMessageBatchIterator[T]) batchMessageIter() iter.Seq2[*LogMessage[T], error] {
	if it.currBatch != nil {
		batch := it.currBatch
		it.currBatch = nil
		return func(yield func(*LogMessage[T], error) bool) {
			for _, messageBlob := range batch {
				message, err := decodeChasmBlob[T](messageBlob.Data)
				if !yield(NewLogMessage(messageBlob.SequenceId, message), err) {
					break
				}
			}
		}
	}

	if it.currNewBatchesIdx < len(it.log.newBatches) {
		batch := it.log.newBatches[it.currNewBatchesIdx]
		it.currNewBatchesIdx++
		return func(yield func(*LogMessage[T], error) bool) {
			for i, message := range batch.messages {
				if !yield(NewLogMessage(batch.batchID+int64(i), message.(T)), nil) {
					break
				}
			}
		}
	}

	panic("iterator has no next value; you must check HasNext() before calling Next()")
}

type LogMessageIterator[T proto.Message] struct {
	ctx context.Context
	log *Log[T]

	token             *tokenspb.HistoryEventRef
	currBatch         []*persistencespb.ChasmLogMessage
	currBatchIdx      int
	currNewBatchesIdx int
}

func newLogMessageIterator[T proto.Message](
	ctx context.Context,
	log *Log[T],
	batchID int64,
) *LogMessageIterator[T] {
	return &LogMessageIterator[T]{
		ctx: ctx,
		log: log,

		token: &tokenspb.HistoryEventRef{
			EventBatchId: batchID,
		},
	}
}

func (it *LogMessageIterator[T]) HasNext() bool {
	if it.ctx.Err() != nil {
		return false
	}
	if it.currBatchIdx < len(it.currBatch) {
		return true
	}
	if it.token.EventBatchId <= it.log.metadata.GetLastSequenceId() {
		token, err := proto.Marshal(it.token)
		if err != nil {
			return false
		}
		batch, err := it.log.node.backend.LoadChasmLogMessages(it.ctx, token, it.log.metadata.GetBranch())
		if err != nil {
			return false
		}
		if len(batch.GetMessages()) == 0 {
			return false
		}
		it.currBatch = batch.Messages
		it.currBatchIdx = 0
		it.token.EventBatchId = batch.Messages[len(batch.Messages)-1].SequenceId + 1
	}
	for it.currNewBatchesIdx < len(it.log.newBatches) &&
		it.log.newBatches[it.currNewBatchesIdx].batchID < it.token.EventBatchId {
		it.currNewBatchesIdx++
	}
	if it.currNewBatchesIdx >= len(it.log.newBatches) {
		return false
	}
	if it.currBatchIdx < len(it.log.newBatches[it.currNewBatchesIdx].messages) {
		return true
	}
	it.currNewBatchesIdx++
	it.currBatchIdx = 0
	return it.currNewBatchesIdx < len(it.log.newBatches)
}

// Returns the next value of the iterator.
// It may panic if HasNext() is not called before each call to Next().
func (it *LogMessageIterator[T]) Next() (*LogMessage[T], error) {
	if it.currBatch != nil {
		if it.currBatchIdx < len(it.currBatch) {
			item := it.currBatch[it.currBatchIdx]
			it.currBatchIdx++
			message, err := decodeChasmBlob[T](item.Data)
			if err != nil {
				return nil, err
			}
			return NewLogMessage(item.SequenceId, message), nil
		}

		panic("iterator has no next value; you must check HasNext() before calling Next()")
	}

	if it.currNewBatchesIdx < len(it.log.newBatches) {
		batch := it.log.newBatches[it.currNewBatchesIdx]
		if it.currBatchIdx < len(batch.messages) {
			message := batch.messages[it.currBatchIdx]
			sequenceID := batch.batchID + int64(it.currBatchIdx)
			it.currBatchIdx++
			return NewLogMessage(sequenceID, message.(T)), nil
		}
	}

	panic("iterator has no next value; you must check HasNext() before calling Next()")
}

type LogMutation struct {
	Branch  *persistencespb.HistoryBranch
	Batches []*LogEventBatchMutation
}

type LogEventBatchMutation struct {
	TxnID     int64
	PrevTxnID int64
	Messages  []*persistencespb.ChasmLogMessage
}
