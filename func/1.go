package main

import "fmt"

func main() {
	fn := func() { fmt.Println("Hello, World!") }
	fn()


	fns := [](func(x int) int) {
		func(x int) int {return x+1},
		func(x int) int {return x+2},
	}
	fmt.Println(fns[1](100))

	d := struct {
		fn func() string
	} {
		fn: func() string { return "my girl" },
	}
	fmt.Println(d.fn())

	fc := make(chan func() string, 2)
	fc <- func() string { return "THIS is a channnel" }
	fmt.Println((<-fc)())
}
