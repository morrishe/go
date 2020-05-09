package main

import (
	"os"
	"fmt"
	"strings"
	"flag"
	"log"
	"io/ioutil"
	"path/filepath"
	"sync"
)

const (
	ENTRYLIMIT = 512 * 1024
	DEPTHLIMIT = 20
	FILENAMELENLIMIT = 128
	OUTPUTFILE = "/tmp/NasWalk.log"
	GOROUTINECOUNTS = 16 
)

var entryLimit, depthLimit, fileNameLenLimit	int
var outputFile string
func init() {
	flag.IntVar(&entryLimit, "entryLimit", ENTRYLIMIT, "entrys limit in a directory")
	flag.IntVar(&depthLimit, "depthLimit", DEPTHLIMIT, "directory depth limit")
	flag.IntVar(&fileNameLenLimit, "fileNameLenLimit", FILENAMELENLIMIT, "filename length limit")
	flag.StringVar(&outputFile, "outputFile", OUTPUTFILE, "the pathname of output file")
}


type DirNode struct {
	FileCounts	int
	DirCounts	int
	Depth		int
	PathName	string
}

func walkDir(dir string, depth int,  n *sync.WaitGroup, ch chan<- DirNode) {
        defer n.Done()
	entrys := dirents(dir)
	var dirCounts, fileCounts int
        for _, entry := range entrys {
                if entry.IsDir() {
			dirCounts++
                        n.Add(1)
                        subdir := filepath.Join(dir, entry.Name())
                        go walkDir(subdir, depth+1, n, ch)
                } else {
			fileCounts++
		}
        }
	var dn DirNode
	dn.FileCounts = fileCounts
	dn.DirCounts = dirCounts
	dn.PathName = dir
	dn.Depth = depth
        ch <- dn
}

var sema = make(chan struct{}, 32)
func dirents(dir string) []os.FileInfo {
        sema <- struct{}{}
        defer func() { <-sema }()

        entries, err := ioutil.ReadDir(dir)
        if err != nil {
                log.Printf("%v\n", err)
                return nil
        }
        return entries
}


func main() {
        flag.Parse()
        roots := flag.Args()
        if len(roots) == 0 {
		fmt.Fprintf(os.Stderr, "USAGE: %s [options] path1 path2...\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
        }

        dnChan := make(chan DirNode)
        var n sync.WaitGroup
        for _, root := range roots {
                n.Add(1)
		absRoot, err := filepath.Abs(root)
		if err != nil {
			log.Println(err)
			continue
		}
		depth := strings.Count(absRoot, "/")
                go walkDir(absRoot, depth, &n, dnChan)
        }

        go func() {
                n.Wait()
                close(dnChan)
        }()

	var maxEntry, maxDepth, maxNameLen int
	var maxdn DirNode
loop:
        for {
                select {
                case dn, ok := <-dnChan:
                        if !ok {
                                break loop
                        }
			//fmt.Printf("DIR:[%s]: depth[%d], files[%d], dirs[%d]\n", dn.PathName, dn.Depth, dn.FileCounts, dn.DirCounts)
			if dn.Depth > maxDepth {
				maxDepth = dn.Depth
				if maxDepth > depthLimit {
					log.Fatalf("DIR:[%s], depth[%d] *** EXCEED LIMIT ***, entry[%d],  files[%d], dirs[%d] \n", dn.PathName, dn.Depth, 
						dn.FileCounts+dn.DirCounts, dn.FileCounts, dn.DirCounts)
				}
				maxdn = dn
			} else if dn.FileCounts + dn.DirCounts > maxEntry {
				maxEntry = dn.FileCounts + dn.DirCounts
				if maxEntry > entryLimit {
					log.Fatalf("DIR:[%s], depth[%d], entry[%d] *** EXCEED LIMIT ***,  files[%d], dirs[%d] \n", dn.PathName, dn.Depth, 
						dn.FileCounts+dn.DirCounts, dn.FileCounts, dn.DirCounts)
				}
				maxdn = dn
			} else if len(filepath.Base(dn.PathName)) > maxNameLen {
				maxNameLen = len(filepath.Base(dn.PathName))
				if maxNameLen > fileNameLenLimit {
					log.Printf("DIR:[%s], depth[%d] exceeds, entry[%d],  files[%d], dirs[%d] \n", dn.PathName, dn.Depth, 
						dn.FileCounts+dn.DirCounts, dn.FileCounts, dn.DirCounts)
				}
				maxdn = dn
			}
			
		default:
                }
        }

	fmt.Printf("MaxDirNode: DIR:[%s]: depth[%d], files[%d], dirs[%d]\n", maxdn.PathName, maxdn.Depth, maxdn.FileCounts, maxdn.DirCounts)
}
