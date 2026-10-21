package main

import (
	"fmt"
	"time"

	queue "github.com/saif177/job-queue"
)

func main() {
	q := queue.New()

	for i := 1; i <= 10; i++ {
		n := i
		q.Enqueue(func() error {
			fmt.Printf("processing job %d\n", n)
			time.Sleep(50 * time.Millisecond)
			return nil
		})
	}

	pool := queue.NewWorkerPool(q, 3, 20*time.Millisecond)
	pool.Start()

	for q.Len() > 0 {
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	pool.Stop()
	fmt.Println("all jobs processed")
}
