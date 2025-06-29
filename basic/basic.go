package basic

import (
	"fmt"
	"sync"
	"time"
)

type Task struct {
	ID   int
	Data string
}

func BasicWorkerPool() {
	fmt.Println("=== 基本的なWorker Pool ===")

	// タスクの数を設定
	numTasks := 10

	// タスクを処理するためのチャネルを作成
	taskChannel := make(chan Task, numTasks)
	// 結果を受け取るためのチャネルを作成
	resultChannel := make(chan string, numTasks)

	// WaitGroupでgoroutineの完了を待つ
	var wg sync.WaitGroup

	// 複数のgoroutineを事前に起動しておく
	numWorkers := 3
	for i := range numWorkers {
		wg.Add(1)
		go worker(i, taskChannel, resultChannel, &wg)
	}

	// タスクをチャネル経由でワーカーに送信
	// 各ワーカーが独立してタスクを処理
	go func() {
		for i := range numTasks {
			task := Task{
				ID:   i,
				Data: fmt.Sprintf("タスク%d", i),
			}
			taskChannel <- task
		}
		close(taskChannel) // タスクの送信完了
	}()

	// すべてのタスクが完了するまで待つ
	go func() {
		wg.Wait()
		close(resultChannel)
	}()

	// 結果を表示
	for result := range resultChannel {
		fmt.Println(result)
	}
}

func worker(id int, taskChannel <-chan Task, resultChannel chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()

	for task := range taskChannel {
		// タスクの処理をシミュレート
		fmt.Printf("ワーカー%d: %sを処理中...\n", id, task.Data)

		// シミュレーションのために少し待つ
		time.Sleep(1 * time.Second)

		// 結果をチャネルに送信
		result := fmt.Sprintf("タスク%d: %s完了", id, task.Data)
		resultChannel <- result
	}

	fmt.Printf("ワーカー%d: 終了\n", id)
}
