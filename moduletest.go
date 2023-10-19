package main

import (
	"fmt"
	"gosrc/module"
)

type Module3 struct {
	Name string
}

func (m *Module3) OnRecv(s string) {
	fmt.Println("call ", s)
}

func NewModule1(id int, name string) module.ModuleBase {
	return &Module3{Name: name}
}

// func main() {
// 	m1 := module.NewModule(1, "m1")
// 	m2 := module.NewModule(2, "m2")
// 	m3 := NewModule1(3, "m3")
// 	m1.(*module.Module1).OnRecv(m1.(*module.Module1).Name)
// 	m2.(*module.Module2).OnRecv(m2.(*module.Module2).Name)
// 	m3.(*Module3).OnRecv(m3.(*Module3).Name)

// }
