package main

import (
	//"bytes"
	"fmt"	
	"os"
)

func basename(s string) string {
	for i := len(s)-1; i >= 0; i-- {
		if s[i] == '/' {
			s = s[i+1:]
			break
		}
	}
	for i := len(s)-1; i >= 0; i-- {
		if s[i] == '.' {
			s = s[:i]
			break
		}
	}
	return s
}

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("USAGE: %s pathname\n", os.Args[0])
		os.Exit(1)
	}

	fmt.Println(basename(os.Args[1]))
}
