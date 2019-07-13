package main

import (
	"os"
	"log"
	"fmt"
	"io"
)

const (
	BufSize = 4096
)

func main() {
	if len(os.Args[1:]) != 2 {
		fmt.Printf("USAGE: %s src dst\n", os.Args[0])
		os.Exit(1)
	}

	fr, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}	
	defer fr.Close()

	fw, err := os.Create(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}
	defer fw.Close()

	var buf = make([]byte, BufSize)
	for {
		n, err := fr.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		_, err = fw.Write(buf[:n])
		if err != nil {
			log.Fatal(err)
		}
	}
}
