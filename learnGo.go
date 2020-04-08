package main

import "fmt"

func main() {
	foo()
}

func foo() {
	defer func() {
		fmt.Println("A")
	}()

	defer func() {
		fmt.Println("B")
	}()
}
