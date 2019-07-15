package main

import (
	//"bytes"
	"fmt"	
	"os"
	//"strings"
)

func comma(s string) string {
	n := len(s)
	if n <= 3 {
		return s
	}
	return comma(s[:n-3]) + "," + s[n-3:]
}

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("USAGE: %s number\n", os.Args[0])
		os.Exit(1)
	}

	fmt.Println(comma(os.Args[1]))
}
