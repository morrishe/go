package main

import (
	"os"
	"log"
	"fmt"
	"io"
	"flag"
)

const (
	BufSize = 4096
)

var ip2 int
func init() {
	flag.IntVar(&ip2, "ip2", 98999999, "kskdskldsklds")
}

func main() {
	var ip = flag.Int("p1", 1234, "help message for flagname")



	flag.Parse()
	fmt.Println(*ip)
	fmt.Println(ip2)
	return



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
