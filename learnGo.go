package main

import "fmt"

type e struct {
	ID   int
	Name string
}

func main() {
	rId := 15

	a := []e{
		{5, "mika"},
		{2, "pera"},
		{3, "laza"},
		{15, "nina"},
		{11, "noa"},
		{7, "mina"},
		{1, "lena"},
	}

	fmt.Println(a)

	for i, sE := range a {
		fmt.Println(i)
		if sE.ID == rId {
			sE.Name = "UPDATE"
			a = append(append(a[:i], sE), a[i+1:]...)
		}
	}

	fmt.Println(a)
}
