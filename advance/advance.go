package advance

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type TaskFunc func(ctx context.Context, data any) (any, error)

type WorkerTask struct {
	ID       int64
	Data     any
	TaskFunc TaskFunc
	Retry    int
	Priority int
}

type TaskResult struct {
	TaskID   int64
	Result   any
	Error    error
	WorkerID int
	Duration time.Duration
}

// 統計情報
type PoolStats struct {
	TotalTasks     int64
	CompletedTasks int64
	FailedTasks    int64
	ActiveWorkers  int64
}

type ProductionWorkerPool struct {
	workerCount int
	taskQueue   chan WorkerTask
	resultQueue chan TaskResult
	stats       PoolStats
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	maxRetries  int
	queueSize   int
	ticker      *time.Ticker
}

func NewProductionWorkerPool(workerCount, queueSize, maxRetries int) *ProductionWorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	return &ProductionWorkerPool{
		workerCount: workerCount,
		taskQueue:   make(chan WorkerTask, queueSize),
		resultQueue: make(chan TaskResult, queueSize),
		maxRetries:  maxRetries,
		queueSize:   queueSize,
		ctx:         ctx,
		cancel:      cancel,
		ticker:      time.NewTicker(5 * time.Second),
	}
}

func (p *ProductionWorkerPool) Start() {
	// 統計監視を開始
	go p.monitorStats()

	// ワーカーを起動
	for i := range p.workerCount {
		p.wg.Add(1)
		go p.worker(i)
	}

	fmt.Printf("Worker Pool開始: %d個のワーカー\n", p.workerCount)
}

func (p *ProductionWorkerPool) Stop() {
	fmt.Println("Worker Pool停止中...")

	p.cancel()
	close(p.taskQueue)

	p.wg.Wait()
	close(p.resultQueue)

	p.ticker.Stop()
	fmt.Println("Worker Pool停止完了")
}

func (p *ProductionWorkerPool) worker(id int) {
	defer p.wg.Done()

	atomic.AddInt64(&p.stats.ActiveWorkers, 1)
	defer atomic.AddInt64(&p.stats.ActiveWorkers, -1)

	fmt.Printf("ワーカー%d: 開始\n", id)

	for {
		select {
		case task, ok := <-p.taskQueue:
			if !ok {
				fmt.Printf("ワーカー%d: タスクキューが閉じられました\n", id)
				return
			}

			// タスクを実行
			result := p.executeTask(id, task)
			p.resultQueue <- result

		case <-p.ctx.Done():
			fmt.Printf("ワーカー%d: コンテキストキャンセル\n", id)
			return
		}
	}
}

func (p *ProductionWorkerPool) executeTask(workerID int, task WorkerTask) TaskResult {
	start := time.Now()

	fmt.Printf("ワーカー%d: タスク%d実行開始\n", workerID, task.ID)

	var result any
	var err error

	// リトライ機能付きでタスクを実行
	for attempt := range p.maxRetries {
		if attempt > 0 {
			fmt.Printf("ワーカー%d: タスク%d リトライ（%d/%d回目）\n", workerID, task.ID, attempt, p.maxRetries)
			//リトライ前に少し待機
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		result, err = task.TaskFunc(p.ctx, task.Data)
		if err == nil {
			break // 成功
		}

		if attempt == p.maxRetries {
			fmt.Printf("ワーカー%d: タスク%d 最大リトライ回数に達しました\n", workerID, task.ID)
		}
	}

	duration := time.Since(start)

	if err != nil {
		atomic.AddInt64(&p.stats.FailedTasks, 1)
		fmt.Printf("ワーカー%d: タスク%d失敗 - %v\n", workerID, task.ID, err)
	} else {
		atomic.AddInt64(&p.stats.CompletedTasks, 1)
		fmt.Printf("ワーカー%d: タスク%d成功\n", workerID, task.ID)
	}

	return TaskResult{
		TaskID:   task.ID,
		Result:   result,
		Error:    err,
		WorkerID: workerID,
		Duration: duration,
	}
}

func (p *ProductionWorkerPool) SubmitTask(data any, taskFunc TaskFunc, priority int) int64 {
	taskID := atomic.AddInt64(&p.stats.TotalTasks, 1)

	task := WorkerTask{
		ID:       taskID,
		Data:     data,
		TaskFunc: taskFunc,
		Priority: priority,
	}

	select {
	case p.taskQueue <- task:
		fmt.Printf("タスク%d: キューに追加\n", taskID)
		return taskID
	case <-p.ctx.Done():
		fmt.Printf("タスク%d: プールが停止中のため追加できません\n", taskID)
		atomic.AddInt64(&p.stats.TotalTasks, -1)
		return -1
	}
}

func (p *ProductionWorkerPool) GetStats() PoolStats {
	return PoolStats{
		TotalTasks:     atomic.LoadInt64(&p.stats.TotalTasks),
		CompletedTasks: atomic.LoadInt64(&p.stats.CompletedTasks),
		FailedTasks:    atomic.LoadInt64(&p.stats.FailedTasks),
		ActiveWorkers:  atomic.LoadInt64(&p.stats.ActiveWorkers),
	}
}

func (p *ProductionWorkerPool) GetResults() <-chan TaskResult {
	return p.resultQueue
}

func (p *ProductionWorkerPool) monitorStats() {
	for {
		select {
		case <-p.ticker.C:
			stats := p.GetStats()
			fmt.Printf("統計 - 総タスク: %d, 完了: %d, 失敗: %d, アクティブワーカー: %d, キュー長: %d\n",
				stats.TotalTasks, stats.CompletedTasks, stats.FailedTasks, stats.ActiveWorkers, len(p.taskQueue))
		case <-p.ctx.Done():
			return
		}
	}
}

// サンプルタスク関数
func sampleHTTPTask(ctx context.Context, data any) (any, error) {
	url := data.(string)

	// HTTPリクエストをシミュレート
	select {
	case <-time.After(time.Duration(1+len(url)%3) * time.Second):
		// 時々エラーを発生させる
		if strings.Contains(url, "7") {
			return nil, fmt.Errorf("HTTP エラー: %s", url)
		}
		return fmt.Sprintf("HTTP レスポンス: %s", url), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func AdvanceWorkerPool() {
	// サンプルタスク
	urls := []string{
		"https://api1.example.com",
		"https://api2.example.com",
		"https://api3.example.com",
		"https://api4.example.com",
		"https://api5.example.com",
		"https://api6.example.com",
		"https://api7.example.com", // これはエラーになる
	}
	numWorkers := 3
	maxRetries := 3

	pool := NewProductionWorkerPool(numWorkers, len(urls), maxRetries)

	// プール開始
	pool.Start()

	// タスクをキューに追加
	go func() {
		for _, url := range urls {
			pool.SubmitTask(url, sampleHTTPTask, 1)
		}
		// 少し待ってから停止
		// contextのキャンセルが動作するか確認用
		time.Sleep(5 * time.Second)
		pool.Stop()
	}()

	// 統計情報を表示
	for result := range pool.GetResults() {
		if result.Error != nil {
			fmt.Printf("タスク%d エラー: %v\n", result.TaskID, result.Error)
		} else {
			fmt.Printf("タスク%d 結果: %v (処理時間: %v)\n", result.TaskID, result.Result, result.Duration)
		}
	}

	// 結果を表示
	finalStats := pool.GetStats()
	fmt.Printf("=== 最終結果 ===\n")
	fmt.Printf("総タスク数: %d\n", finalStats.TotalTasks)
	fmt.Printf("成功タスク: %d\n", finalStats.CompletedTasks)
	fmt.Printf("失敗タスク: %d\n", finalStats.FailedTasks)
}
