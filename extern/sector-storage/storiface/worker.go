package storiface

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
)

type WorkerInfo struct {
	Hostname string

	Resources       WorkerResources
	taskResourcesLk sync.Mutex
	TaskResources   map[sealtasks.TaskType]*TaskConfig
}

type WorkerResources struct {
	MemPhysical uint64
	MemSwap     uint64

	MemReserved uint64 // Used by system / other processes

	CPUs uint64 // Logical cores
	GPUs []string
}

type WorkerStats struct {
	Info    WorkerInfo
	Enabled bool

	MemUsedMin uint64
	MemUsedMax uint64
	GpuUsed    bool   // nolint
	CpuUse     uint64 // nolint
}

type TaskConfig struct {
	taskType   sealtasks.TaskType
	limitCount int
	runCount   int
}

const (
	RWRetWait  = -1
	RWReturned = -2
	RWRetDone  = -3
)

type WorkerJob struct {
	ID     CallID
	Sector abi.SectorID
	Task   sealtasks.TaskType

	// 1+ - assigned
	// 0  - running
	// -1 - ret-wait
	// -2 - returned
	// -3 - ret-done
	RunWait int
	Start   time.Time

	Hostname string `json:",omitempty"` // optional, set for ret-wait jobs
}

type CallID struct {
	Sector abi.SectorID
	ID     uuid.UUID
}

func (c CallID) String() string {
	return fmt.Sprintf("%d-%d-%s", c.Sector.Miner, c.Sector.Number, c.ID)
}

var _ fmt.Stringer = &CallID{}

var UndefCall CallID

type WorkerCalls interface {
	AddPiece(ctx context.Context, sector storage.SectorRef, pieceSizes []abi.UnpaddedPieceSize, newPieceSize abi.UnpaddedPieceSize, pieceData storage.Data) (CallID, error)
	SealPreCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, pieces []abi.PieceInfo) (CallID, error)
	SealPreCommit2(ctx context.Context, sector storage.SectorRef, pc1o storage.PreCommit1Out) (CallID, error)
	SealCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (CallID, error)
	SealCommit2(ctx context.Context, sector storage.SectorRef, c1o storage.Commit1Out) (CallID, error)
	FinalizeSector(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) (CallID, error)
	ReleaseUnsealed(ctx context.Context, sector storage.SectorRef, safeToFree []storage.Range) (CallID, error)
	MoveStorage(ctx context.Context, sector storage.SectorRef, types SectorFileType) (CallID, error)
	UnsealPiece(context.Context, storage.SectorRef, UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) (CallID, error)
	ReadPiece(context.Context, io.Writer, storage.SectorRef, UnpaddedByteIndex, abi.UnpaddedPieceSize) (CallID, error)
	Fetch(context.Context, storage.SectorRef, SectorFileType, PathType, AcquireMode) (CallID, error)
}

type ErrorCode int

const (
	ErrUnknown ErrorCode = iota
)

const (
	// Temp Errors
	ErrTempUnknown ErrorCode = iota + 100
	ErrTempWorkerRestart
	ErrTempAllocateSpace
)

type CallError struct {
	Code    ErrorCode
	Message string
	sub     error
}

func (c *CallError) Error() string {
	return fmt.Sprintf("storage call error %d: %s", c.Code, c.Message)
}

func (c *CallError) Unwrap() error {
	if c.sub != nil {
		return c.sub
	}

	return errors.New(c.Message)
}

func Err(code ErrorCode, sub error) *CallError {
	return &CallError{
		Code:    code,
		Message: sub.Error(),

		sub: sub,
	}
}

type WorkerReturn interface {
	ReturnAddPiece(ctx context.Context, callID CallID, pi abi.PieceInfo, err *CallError) error
	ReturnSealPreCommit1(ctx context.Context, callID CallID, p1o storage.PreCommit1Out, err *CallError) error
	ReturnSealPreCommit2(ctx context.Context, callID CallID, sealed storage.SectorCids, err *CallError) error
	ReturnSealCommit1(ctx context.Context, callID CallID, out storage.Commit1Out, err *CallError) error
	ReturnSealCommit2(ctx context.Context, callID CallID, proof storage.Proof, err *CallError) error
	ReturnFinalizeSector(ctx context.Context, callID CallID, err *CallError) error
	ReturnReleaseUnsealed(ctx context.Context, callID CallID, err *CallError) error
	ReturnMoveStorage(ctx context.Context, callID CallID, err *CallError) error
	ReturnUnsealPiece(ctx context.Context, callID CallID, err *CallError) error
	ReturnReadPiece(ctx context.Context, callID CallID, ok bool, err *CallError) error
	ReturnFetch(ctx context.Context, callID CallID, err *CallError) error
}

func NewTaskLimitConfig() map[sealtasks.TaskType]*TaskConfig {
	taskLimitCount := 20
	config := map[sealtasks.TaskType]*TaskConfig{}
	config[sealtasks.TTAddPiece] = newTaskLimitConfig(taskLimitCount, sealtasks.TTAddPiece)
	config[sealtasks.TTPreCommit1] = newTaskLimitConfig(taskLimitCount, sealtasks.TTPreCommit1)
	config[sealtasks.TTPreCommit2] = newTaskLimitConfig(taskLimitCount, sealtasks.TTPreCommit2)
	config[sealtasks.TTCommit1] = newTaskLimitConfig(taskLimitCount, sealtasks.TTCommit1)
	config[sealtasks.TTCommit2] = newTaskLimitConfig(taskLimitCount, sealtasks.TTCommit2)
	config[sealtasks.TTFinalize] = newTaskLimitConfig(taskLimitCount, sealtasks.TTFinalize)
	config[sealtasks.TTFetch] = newTaskLimitConfig(taskLimitCount, sealtasks.TTFetch)
	config[sealtasks.TTUnseal] = newTaskLimitConfig(taskLimitCount, sealtasks.TTUnseal)
	config[sealtasks.TTReadUnsealed] = newTaskLimitConfig(taskLimitCount, sealtasks.TTReadUnsealed)

	if ap, ok := os.LookupEnv("ap-task-limit"); ok {
		if apCnt, e := strconv.Atoi(ap); e == nil {
			config[sealtasks.TTAddPiece] = newTaskLimitConfig(apCnt, sealtasks.TTAddPiece)
		}
	}

	// TODO 需要补充

	return config
}

func newTaskLimitConfig(limitCount int, taskType sealtasks.TaskType) *TaskConfig {
	return &TaskConfig{
		taskType:   taskType,
		limitCount: limitCount,
		runCount:   0,
	}
}

func (wi *WorkerInfo) TaskAddOne(taskType sealtasks.TaskType) {
	wi.taskResourcesLk.Lock()
	defer wi.taskResourcesLk.Unlock()
	if cnt, ok := wi.TaskResources[taskType]; ok {
		cnt.runCount++
	}
}

func (wi *WorkerInfo) TaskReduceOne(taskType sealtasks.TaskType) {
	wi.taskResourcesLk.Lock()
	defer wi.taskResourcesLk.Unlock()
	if cnt, ok := wi.TaskResources[taskType]; ok {
		cnt.runCount--
	}
}

func (wi *WorkerInfo) GetTaskRunCount(taskType sealtasks.TaskType) int {
	wi.taskResourcesLk.Lock()
	defer wi.taskResourcesLk.Unlock()
	if cnt, ok := wi.TaskResources[taskType]; ok {
		return cnt.runCount
	}

	return 0
}

func (wi *WorkerInfo) GetTaskLimitCount(taskType sealtasks.TaskType) int {
	wi.taskResourcesLk.Lock()
	defer wi.taskResourcesLk.Unlock()
	if cnt, ok := wi.TaskResources[taskType]; ok {
		return cnt.limitCount
	}

	return 0
}

func (wi *WorkerInfo) GetTaskFreeCount(taskType sealtasks.TaskType) int {
	wi.taskResourcesLk.Lock()
	defer wi.taskResourcesLk.Unlock()
	if cnt, ok := wi.TaskResources[taskType]; ok {
		return cnt.limitCount - cnt.runCount
	}

	return 0
}
