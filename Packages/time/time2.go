package main

import (
	"fmt"
	"time"
)

func timestampDemo() {
	now := time.Now()
	fmt.Println(now)
	timeS := now.Unix()
	timeNS := now.UnixNano()
	fmt.Printf("ms: %v\n", timeS)
	fmt.Printf("ns: %v\n", timeNS)

	now2 := time.Unix(timeS, 0)
	fmt.Println(now2)
}

func main() {
	timestampDemo()
	fmt.Println(time.Second)
	fmt.Println(time.Hour)
}
