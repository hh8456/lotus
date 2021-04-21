package sectorstorage

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0
var SelectorTimeout = 5 * time.Second
var InitWait = 3 * time.Second

var (
	SchedWindows = 2
)

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *workerHandle) (bool, error) // true if worker is acceptable for performing a task

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) // true if a is preferred over b
	FindDataWorker(ctx context.Context, task sealtasks.TaskType, sid abi.SectorID, spt abi.RegisteredSealProof, whnd *workerHandle) bool
}

type scheduler struct {
	workersLk sync.RWMutex
	workers   map[WorkerID]*workerHandle

	schedule       chan *workerRequest
	windowRequests chan *schedWindowRequest // 主动触发调度程序执行, jump to: sched.go - func (sh *scheduler) runSched()
	workerChange   chan struct{}            // worker added / changed/freed resources
	workerDisable  chan workerDisableReq    // worker 断线

	// owned by the sh.runSched goroutine
	schedQueue  *requestQueue         // 质押扇区, 下发 p1,p2,c1,c2 等任务时,会往 schedQueue 写入数据
	openWindows []*schedWindowRequest // 保存 windowRequests 中的信息, openWindows 中要有数据,才能驱动调度程序

	workTracker *workTracker

	info chan func(interface{})

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing
}

type workerHandle struct {
	workerRpc Worker

	info storiface.WorkerInfo

	preparing *activeResources
	active    *activeResources

	lk sync.Mutex

	wndLk sync.Mutex
	// 官方原版代码 sched_worker.go - type schedWorker struct - scheduledWindows 我放弃使用
	// 就没有其他地方写 activeWindows, 直接导致 worker.activeWindows 未使用
	activeWindows []*schedWindow

	enabled bool

	// for sync manager goroutine closing
	cleanupStarted bool
	closedMgr      chan struct{}
	closingMgr     chan struct{}
	workerOnFree   chan struct{}
}

type schedWindowRequest struct {
	worker WorkerID
	// 由于弃用官方调度源码 func (sh *scheduler) trySched1() 和
	// sched_worker.go - func (sw *schedWorker) requestWindows() bool,
	// 就没有其他地方往 done 压入数据而触发 sched.go - runSched()
	done chan *schedWindow
}

type schedWindow struct {
	allocated activeResources
	todo      []*workerRequest
}

type workerDisableReq struct {
	activeWindows []*schedWindow
	wid           WorkerID
	done          func()
}

type activeResources struct {
	memUsedMin uint64
	memUsedMax uint64
	gpuUsed    bool
	cpuUse     uint64

	cond *sync.Cond
}

type workerRequest struct {
	sector   storage.SectorRef
	taskType sealtasks.TaskType
	priority int // larger values more important
	sel      WorkerSelector

	prepare WorkerAction
	work    WorkerAction

	start time.Time

	index int // The index of the item in the heap.

	indexHeap int
	ret       chan<- workerResponse
	ctx       context.Context
}

type workerResponse struct {
	err error
}

func newScheduler() *scheduler {
	return &scheduler{
		workers: map[WorkerID]*workerHandle{},

		schedule:       make(chan *workerRequest),
		windowRequests: make(chan *schedWindowRequest, 20),
		workerChange:   make(chan struct{}, 20),
		workerDisable:  make(chan workerDisableReq),

		schedQueue: &requestQueue{},

		workTracker: &workTracker{
			done:    map[storiface.CallID]struct{}{},
			running: map[storiface.CallID]trackedWork{},
		},

		info: make(chan func(interface{})),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (sh *scheduler) Schedule(ctx context.Context, sector storage.SectorRef, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &workerRequest{
		sector:   sector,
		taskType: taskType,
		priority: getPriority(ctx),
		sel:      sel,

		prepare: prepare,
		work:    work,

		start: time.Now(),

		ret: ret,
		ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

type SchedDiagRequestInfo struct {
	Sector   abi.SectorID
	TaskType sealtasks.TaskType
	Priority int
}

type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []string
}

func (sh *scheduler) runSched() {
	defer close(sh.closed)

	iw := time.After(InitWait)
	var initialised bool

	for {
		var doSched bool
		// toDisable 是断线的 worker
		var toDisable []workerDisableReq

		select {
		// worker 完成任务时接到通知:
		// seal/v0/addpiece - seal/v0/precommit/1 - seal/v0/precommit/2 - seal/v0/commit/1 - seal/v0/commit/2
		// FinalizeSector - Fetch
		case <-sh.workerChange:
			doSched = true
			// worker 断线
		case dreq := <-sh.workerDisable:
			toDisable = append(toDisable, dreq)
			doSched = true
		case req := <-sh.schedule:
			sh.schedQueue.Push(req)
			doSched = true

			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}
		case req := <-sh.windowRequests:
			sh.openWindows = append(sh.openWindows, req)
			doSched = true
		case ireq := <-sh.info:
			ireq(sh.diag())

		case <-iw:
			initialised = true
			iw = nil
			doSched = true
		case <-sh.closing:
			sh.schedClose()
			return
		}

		if doSched && initialised {
			// First gather any pending tasks, so we go through the scheduling loop
			// once for every added task

		loop:
			for {
				select {
				case <-sh.workerChange:
				case dreq := <-sh.workerDisable:
					toDisable = append(toDisable, dreq)
				case req := <-sh.schedule:
					sh.schedQueue.Push(req)
					if sh.testSync != nil {
						sh.testSync <- struct{}{}
					}
				case req := <-sh.windowRequests:
					sh.openWindows = append(sh.openWindows, req)
				default:
					break loop
				}
			}

			// worker 断线后,必然会执行下面的 for 循环
			for _, req := range toDisable {
				// 不质押扇区, worker 断线后,并不会执行下面的 for 循环
				for _, window := range req.activeWindows {
					for _, request := range window.todo {
						sh.schedQueue.Push(request)
					}
				}

				openWindows := make([]*schedWindowRequest, 0, len(sh.openWindows))
				for _, window := range sh.openWindows {
					if window.worker != req.wid {
						openWindows = append(openWindows, window)
					}
				}
				sh.openWindows = openWindows

				sh.workersLk.Lock()
				sh.workers[req.wid].enabled = false
				sh.workersLk.Unlock()

				req.done()
			}

			sh.trySched()
		}

	}
}

func (sh *scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.sector.ID,
			TaskType: task.taskType,
			Priority: task.priority,
		})
	}

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	for _, window := range sh.openWindows {
		out.OpenWindows = append(out.OpenWindows, uuid.UUID(window.worker).String())
	}

	return out
}

func (sh *scheduler) trySched() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()

	log.Debugf("trySched %d queued", sh.schedQueue.Len())
	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ { // 遍历任务列表
		task := (*sh.schedQueue)[sqi]

		tried := 0
		var acceptable []WorkerID
		var freetable []int
		best := 0
		localWorker := false
		for wid, worker := range sh.workers {
			if !worker.enabled {
				continue
			}

			// p1/p2/c1 放一个 worker 处理
			if task.taskType == sealtasks.TTPreCommit1 || task.taskType == sealtasks.TTPreCommit2 || task.taskType == sealtasks.TTCommit1 {
				if isExist := task.sel.FindDataWorker(task.ctx, task.taskType, task.sector.ID, task.sector.ProofType, worker); !isExist {
					log.Infof("%+v jobTask task.sel.Ok return false ", task.taskType)
					continue
				}
			}

			ok, err := task.sel.Ok(task.ctx, task.taskType, task.sector.ProofType, worker)
			if err != nil || !ok {
				log.Infof("%+v jobTask task.sel.Ok return false ", task.taskType)
				continue
			}

			freecount := sh.getTaskFreeCount(wid, task.taskType)
			if freecount <= 0 {
				log.Infof("%+v jobTask freecount is 0 ", task.taskType)
				continue
			}
			tried++

			// 根据 freetable acceptable 来查询到最空闲的 worker
			freetable = append(freetable, freecount)
			acceptable = append(acceptable, wid)

			if isExist := task.sel.FindDataWorker(task.ctx, task.taskType, task.sector.ID, task.sector.ProofType, worker); isExist {
				localWorker = true
				break
			}
		}

		if len(acceptable) > 0 {
			if localWorker {
				best = len(acceptable) - 1
			} else {
				// 选出空闲数最大的 max freecount
				max := 0
				for i, v := range freetable {
					if v > max {
						max = v
						best = i
					}
				}
			}

			wid := acceptable[best]
			whl := sh.workers[wid]
			log.Infof("huanghai, worker %s , worker id: %s, will be do the %+v jobTask!", whl.info.Hostname, wid, task.taskType)
			sh.schedQueue.Remove(sqi)
			sqi--
			if err := sh.assignWorker(wid, whl, task); err != nil {
				log.Error("assignWorker error: %+v", err)
				go task.respond(xerrors.Errorf("assignWorker error: %w", err))
			}
		}

		if tried == 0 {
			log.Infof("no worker do the %+v jobTask!", task.taskType)
		}
	}
}

func (sh *scheduler) trySched1() {
	/*
		This assigns tasks to workers based on:
		- Task priority (achieved by handling sh.schedQueue in order, since it's already sorted by priority)
		- Worker resource availability
		- Task-specified worker preference (acceptableWindows array below sorted by this preference)
		- Window request age

		1. For each task in the schedQueue find windows which can handle them
		1.1. Create list of windows capable of handling a task
		1.2. Sort windows according to task selector preferences
		2. Going through schedQueue again, assign task to first acceptable window
		   with resources available
		3. Submit windows with scheduled tasks to workers

	*/

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	windowsLen := len(sh.openWindows)
	queuneLen := sh.schedQueue.Len()

	log.Debugf("SCHED %d queued; %d open windows", queuneLen, windowsLen)

	if windowsLen == 0 || queuneLen == 0 {
		// nothing to schedule on
		return
	}

	windows := make([]schedWindow, windowsLen)
	acceptableWindows := make([][]int, queuneLen)

	// Step 1
	throttle := make(chan struct{}, windowsLen)

	var wg sync.WaitGroup
	wg.Add(queuneLen)
	for i := 0; i < queuneLen; i++ {
		throttle <- struct{}{}

		go func(sqi int) {
			defer wg.Done()
			defer func() {
				<-throttle
			}()

			task := (*sh.schedQueue)[sqi]
			//log.Debugf("huanghai, 调度任务类型: %s", task.taskType)

			// TODO
			for wid, worker := range sh.workers {
				if !worker.enabled {
					log.Info(" worker id: %s, worker.enabled = false", wid)
					continue
				}

				task.sel.FindDataWorker(task.ctx, task.taskType, task.sector.ID, task.sector.ProofType, worker)
			}

			needRes := ResourceTable[task.taskType][task.sector.ProofType]

			task.indexHeap = sqi
			for wnd, windowRequest := range sh.openWindows {
				worker, ok := sh.workers[windowRequest.worker]
				if !ok {
					log.Errorf("worker referenced by windowRequest not found (worker: %s)", windowRequest.worker)
					// TODO: How to move forward here?
					continue
				}

				if !worker.enabled {
					log.Debugw("skipping disabled worker", "worker", windowRequest.worker)
					continue
				}

				// TODO: allow bigger windows
				if !windows[wnd].allocated.canHandleRequest(needRes, windowRequest.worker, "schedAcceptable", worker.info.Resources) {
					continue
				}

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				ok, err := task.sel.Ok(rpcCtx, task.taskType, task.sector.ProofType, worker)
				cancel()
				if err != nil {
					log.Errorf("trySched(1) req.sel.Ok error: %+v", err)
					continue
				}

				if !ok {
					continue
				}

				acceptableWindows[sqi] = append(acceptableWindows[sqi], wnd)
			}

			if len(acceptableWindows[sqi]) == 0 {
				return
			}

			// Pick best worker (shuffle in case some workers are equally as good)
			rand.Shuffle(len(acceptableWindows[sqi]), func(i, j int) {
				acceptableWindows[sqi][i], acceptableWindows[sqi][j] = acceptableWindows[sqi][j], acceptableWindows[sqi][i] // nolint:scopelint
			})
			sort.SliceStable(acceptableWindows[sqi], func(i, j int) bool {
				wii := sh.openWindows[acceptableWindows[sqi][i]].worker // nolint:scopelint
				wji := sh.openWindows[acceptableWindows[sqi][j]].worker // nolint:scopelint

				if wii == wji {
					// for the same worker prefer older windows
					return acceptableWindows[sqi][i] < acceptableWindows[sqi][j] // nolint:scopelint
				}

				wi := sh.workers[wii]
				wj := sh.workers[wji]

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				defer cancel()

				r, err := task.sel.Cmp(rpcCtx, task.taskType, wi, wj)
				if err != nil {
					log.Errorf("selecting best worker: %s", err)
				}
				return r
			})
		}(i)
	}

	wg.Wait()

	log.Debugf("SCHED windows: %+v", windows)
	log.Debugf("SCHED Acceptable win: %+v", acceptableWindows)

	// Step 2
	scheduled := 0
	rmQueue := make([]int, 0, queuneLen)

	for sqi := 0; sqi < queuneLen; sqi++ {
		task := (*sh.schedQueue)[sqi]
		needRes := ResourceTable[task.taskType][task.sector.ProofType]

		selectedWindow := -1
		for _, wnd := range acceptableWindows[task.indexHeap] {
			wid := sh.openWindows[wnd].worker
			wr := sh.workers[wid].info.Resources

			log.Debugf("SCHED try assign sqi:%d sector %d to window %d", sqi, task.sector.ID.Number, wnd)

			// TODO: allow bigger windows
			if !windows[wnd].allocated.canHandleRequest(needRes, wid, "schedAssign", wr) {
				continue
			}

			log.Debugf("SCHED ASSIGNED sqi:%d sector %d task %s to window %d", sqi, task.sector.ID.Number, task.taskType, wnd)

			windows[wnd].allocated.add(wr, needRes)
			// TODO: We probably want to re-sort acceptableWindows here based on new
			//  workerHandle.utilization + windows[wnd].allocated.utilization (workerHandle.utilization is used in all
			//  task selectors, but not in the same way, so need to figure out how to do that in a non-O(n^2 way), and
			//  without additional network roundtrips (O(n^2) could be avoided by turning acceptableWindows.[] into heaps))

			selectedWindow = wnd
			break
		}

		if selectedWindow < 0 {
			// all windows full
			continue
		}

		windows[selectedWindow].todo = append(windows[selectedWindow].todo, task)

		rmQueue = append(rmQueue, sqi)
		scheduled++
	}

	if len(rmQueue) > 0 {
		for i := len(rmQueue) - 1; i >= 0; i-- {
			sh.schedQueue.Remove(rmQueue[i])
		}
	}

	// Step 3

	if scheduled == 0 {
		return
	}

	scheduledWindows := map[int]struct{}{}
	for wnd, window := range windows {
		if len(window.todo) == 0 {
			// Nothing scheduled here, keep the window open
			continue
		}

		scheduledWindows[wnd] = struct{}{}

		window := window // copy
		select {
		case sh.openWindows[wnd].done <- &window:
		default:
			log.Error("expected sh.openWindows[wnd].done to be buffered")
		}
	}

	// Rewrite sh.openWindows array, removing scheduled windows
	newOpenWindows := make([]*schedWindowRequest, 0, windowsLen-len(scheduledWindows))
	for wnd, window := range sh.openWindows {
		if _, scheduled := scheduledWindows[wnd]; scheduled {
			// keep unscheduled windows open
			continue
		}

		newOpenWindows = append(newOpenWindows, window)
	}

	sh.openWindows = newOpenWindows
}

func (sh *scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	log.Debugf("closing scheduler")

	for i, w := range sh.workers {
		sh.workerCleanup(i, w)
	}
}

func (sh *scheduler) Info(ctx context.Context) (interface{}, error) {
	ch := make(chan interface{}, 1)

	sh.info <- func(res interface{}) {
		ch <- res
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sh *scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// 这里是从 sched_worker.go - func (sw *schedWorker) startProcessingTask 修改的
func (sh *scheduler) assignWorker(wid WorkerID, w *workerHandle, req *workerRequest) error {
	sh.taskAddOne(wid, req.taskType)
	needRes := ResourceTable[req.taskType][req.sector.ProofType]

	w.lk.Lock()
	w.preparing.add(w.info.Resources, needRes)
	w.lk.Unlock()

	go func() {
		err := req.prepare(req.ctx, sh.workTracker.worker(wid, w.workerRpc)) // fetch扇区
		sh.workersLk.Lock()

		if err != nil {
			sh.taskReduceOne(wid, req.taskType)
			w.lk.Lock()
			w.preparing.free(w.info.Resources, needRes)
			w.lk.Unlock()
			sh.workersLk.Unlock()

			select {
			case w.workerOnFree <- struct{}{}:
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond (prepare error: %+v)", err)
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}
			return
		}

		err = w.active.withResources(wid, w.info.Resources, needRes, &sh.workersLk, func() error {
			w.lk.Lock()
			w.preparing.free(w.info.Resources, needRes)
			w.lk.Unlock()
			sh.workersLk.Unlock()
			defer sh.workersLk.Lock() // we MUST return locked from this function

			select {
			case w.workerOnFree <- struct{}{}:
			case <-sh.closing:
			}

			err = req.work(req.ctx, sh.workTracker.worker(wid, w.workerRpc))
			sh.taskReduceOne(wid, req.taskType)

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond")
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response")
			}

			return nil
		})

		sh.workersLk.Unlock()

		// This error should always be nil, since nothing is setting it, but just to be safe:
		if err != nil {
			log.Errorf("error executing worker (withResources): %+v", err)
		}
	}()

	return nil
}

// p2, c2 使用 GPU
// p1, c1 使用 cpu
func (sh *scheduler) getTaskFreeCount(wid WorkerID, taskType sealtasks.TaskType) int {
	whl := sh.workers[wid]
	freeCount := whl.info.GetTaskFreeCount(taskType)
	runCount := whl.info.GetTaskRunCount(taskType)

	log.Debugf("huanghai, %+v jobTask freecount is %d, runCount is %d ", taskType, freeCount, runCount)

	//  可以同时运行多个 ap, p1 任务
	if taskType == sealtasks.TTAddPiece || taskType == sealtasks.TTPreCommit1 {
		if freeCount > 0 {
			return freeCount
		}

		return 0
	}

	// 只能同时运行一个 p2 任务, 因为要独占显卡, 并且要和 c2 互斥运行
	if taskType == sealtasks.TTPreCommit2 {
		// 不能有 p2 任务在运行
		if runCount > 0 {
			log.Infof("worker %s already doing p2 taskjob, can not run p2 "+
				"taskjob again, p2 必须独占显卡,但已经有 p2 任务在运行了", wid)
			return 0
		}

		// 不能有 C2 任务在运行
		if whl.info.GetTaskRunCount(sealtasks.TTCommit2) > 0 {
			log.Infof("worker %s already doing c2 taskjob, can not run p2 "+
				"taskjob, p2 必须独占显卡,但已经有 c2 任务在占用显卡运行了", wid)
			return 0
		}

		if freeCount > 0 && runCount == 0 {
			return freeCount
		}

		log.Infof("worker %s want todo c2 taskjob, but fail. "+
			"freeCount = %d( want to > 0 ), runCount = %d( want to 0 )", wid)

		return 0
	}

	// 只能同时运行一个 c1 任务, 避免多个 c1 抢占CPU资源
	if taskType == sealtasks.TTCommit1 {
		if runCount > 0 {
			log.Infof("worker %s already doing c1 taskjob, can not run c1 "+
				"taskjob again, 只能有一个 c1 任务运行, 但已经有 c1 任务在运行了", wid)
			return 0
		}

		if freeCount > 0 && runCount == 0 {
			return freeCount
		}

		log.Infof("worker %s want todo c1 taskjob, but fail. "+
			"freeCount = %d( want to > 0 ), runCount = %d( want to 0 )", wid)

		return 0
	}

	// 只能同时运行一个 c2 任务, 因为要独占显卡, 并且要和 p2 互斥运行
	if taskType == sealtasks.TTCommit2 {
		// 不能有 c2 任务在运行
		if runCount > 0 {
			log.Infof("worker %s already doing c2 taskjob, can not run c2 "+
				"taskjob again, c2 必须独占显卡,但已经有 c2 任务在运行了", wid)
			return 0
		}

		// 不能有 p2 任务在运行
		if whl.info.GetTaskRunCount(sealtasks.TTPreCommit2) > 0 {
			log.Infof("worker %s already doing p2 taskjob, can not run c2 "+
				"taskjob, c2 必须独占显卡,但已经有 p2 任务在占用显卡运行了", wid)
			return 0
		}

		if freeCount > 0 && runCount == 0 {
			return freeCount
		}

		log.Infof("worker %s want todo c2 taskjob, but fail. "+
			"freeCount = %d( want to > 0 ), runCount = %d( want to 0 )", wid)

		return 0
	}

	// 不限制
	if taskType == sealtasks.TTFinalize ||
		taskType == sealtasks.TTFetch ||
		taskType == sealtasks.TTUnseal ||
		taskType == sealtasks.TTReadUnsealed {

		return 1
	}

	return 0
}

func (sh *scheduler) taskAddOne(wid WorkerID, taskType sealtasks.TaskType) {
	whl := sh.workers[wid]
	whl.info.TaskAddOne(taskType)
}

func (sh *scheduler) taskReduceOne(wid WorkerID, taskType sealtasks.TaskType) {
	whl := sh.workers[wid]
	whl.info.TaskReduceOne(taskType)
}
