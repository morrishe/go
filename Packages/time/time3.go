package main

import (
	"fmt"
	"time"
)

func main() {
	now := time.Now()
	later := now.Add(time.Hour)
	fmt.Println("", now, "\n", later)
}
