package main

import (
	//"bytes"
	"fmt"	
	"os"
	"strings"
)

func basename(s string) string {
	slash := strings.LastIndex(s, "/")
	s = s[slash+1:]
	if dot := strings.LastIndex(s, "."); dot >= 0 {
		s = s[:dot]
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
