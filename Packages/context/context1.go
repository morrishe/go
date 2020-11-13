package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

var wg	sync.WaitGroup

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go worker(ctx, i)
	}
	time.Sleep(time.Second * 3)
	cancel()
	wg.Wait()
	fmt.Println("over")
}

func worker(ctx context.Context, i int) {
LOOP:
	for {
		fmt.Println("worker", i)
		time.Sleep(time.Second)
		select {
			case <- ctx.Done():
				break LOOP
			default:
		}
	}

	wg.Done()
}
		
