package main

import (
	"fmt"
	"os"
	"io"
)

func main() {
	var dstFile, srcFile string
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "%s srcFile dstFile\n", os.Args[0])
		os.Exit(1)
	}	
	srcFile = os.Args[1]
	dstFile = os.Args[2]

	srcf, err := os.Open(srcFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "os.Open('%s') error: %v\n", srcFile, err)
		os.Exit(1)
	}
	dstf, err := os.Create(dstFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "os.Create('%s') error: %v\n", dstFile, err)
		os.Exit(1)
	}

	buf := make([]byte, 1024, 4096)
	for {
		n, err := srcf.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "*file.Read() error: %v\n", err)
			os.Exit(2)
		}
		_, err = dstf.Write(buf[:n])
		if err != nil {
			fmt.Fprintf(os.Stderr, "*file.Write() error: %v\n", err)
			os.Exit(2)
		}
	}
	srcf.Close()
	dstf.Close()
}
