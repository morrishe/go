package main

import (
	"fmt"
	"time"
)

func timeDemo() {
	now := time.Now()
	fmt.Printf("current time: %v\n", now)
	
	year := now.Year()
	month := now.Month()
	day := now.Day()
	hour := now.Hour()
	weekday := now.Weekday()
	minute := now.Minute()
	second := now.Second()
	fmt.Printf("%d-%02d-%02d %02d:%02d:%02d, %v\n", year, month, day, hour, minute, second, weekday)
}

func main() {
	timeDemo()
}
