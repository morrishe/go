package main

import (
	"fmt"
	"time"
)

func main() {
	ticker := time.Tick(time.Second)
	for i := range ticker {
		fmt.Println(i)
	}
}
