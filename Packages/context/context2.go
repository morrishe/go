package main

import (
	"context"
	"fmt"
	"time"
)

func gen(ctx context.Context) <-chan int {
	dst := make(chan int)
	n := 1
	go func() {
		for {
			select {
			case <- ctx.Done():
				return
			case dst <- n:
				n++
			}
		}
	}()
	return dst
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	for n := range gen(ctx) {
		fmt.Println(n)
		time.Sleep(time.Millisecond * 500)
		if n == 5 {
			break
		}
	}
}
