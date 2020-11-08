package main

import (
	"fmt"
	"net"
	"bufio"

)

func process(conn net.Conn) {
	defer conn.Close()
	for {
		reader := bufio.NewReader(conn)
		var buf [128]byte
		n, err := reader.Read(buf[:])
		if err != nil {
			fmt.Println("read from client failed, err:", err)
			break
		}
		recvStr := string(buf[:n])
		fmt.Println("received client data: ", recvStr)
		conn.Write([]byte(recvStr))
	}
}


func main() {
	listen, err := net.Listen("tcp", ":20000")
	if err != nil {
		fmt.Println("listen() failed, err: ", err)
		return
	}
	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("accept failed, err: ", err)
			continue
		}
		go process(conn)
	}
}
		
