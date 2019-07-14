package main

import (
	"os"
	"log"
	"fmt"
	"io"
	"flag"
)

const (
	Buf_4K = 4096 * (1<<iota)
	Buf_8K
	Buf_16K
	Buf_32K
	Buf_64K
)

var bs int
func init() {
	flag.IntVar(&bs, "bs", Buf_4K, "Copy Buf size")
}

func main() {
	flag.Parse()
	
	var BufSize = Buf_4K
	switch bs {
	case 0:
		BufSize = Buf_4K
	case 1:
		BufSize = Buf_8K
	case 2:
		BufSize = Buf_16K
	case 3:
		BufSize = Buf_32K
	case 4:
		BufSize = Buf_64K
	default:
		BufSize = Buf_4K
	}

	if len(flag.Args()) != 2 {
		fmt.Printf("USAGE: %s src dst\n", os.Args[0])
		os.Exit(1)
	}

	fr, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}	
	defer fr.Close()

	fw, err := os.Create(flag.Arg(1))
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
