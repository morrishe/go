package main

import (
	"fmt"
	"context"
	"time"
)

func main() {
	d := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()
	
	select {
	case <- time.After(1 * time.Second):
		fmt.Println("over slept")
	case <- ctx.Done():
		fmt.Println(ctx.Err())
	}
}
	
