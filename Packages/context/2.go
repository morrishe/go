package main

import (
	"fmt"
	"sync"
	"time"
)

var wg sync.WaitGroup

func worker(exitChan chan struct{}) {
LOOP:
	for {
		fmt.Println("worker")
		time.Sleep(time.Second)
		select {
			case <- exitChan:
				break LOOP
			default:
		}
	}
	wg.Done()
}

func main() {
	var exitChan = make(chan struct{})
	wg.Add(1)
	go worker(exitChan)
	time.Sleep(time.Second * 3)
	exitChan <- struct{}{}
	close(exitChan)
	wg.Wait()
	fmt.Println("over")
}
