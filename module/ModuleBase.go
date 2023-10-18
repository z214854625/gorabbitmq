package module

import (
	"fmt"
)

type ModuleBase interface {
	OnRecv(string)
}

type Module1 struct {
	Name string
}

func (m *Module1) OnRecv(s string) {
	fmt.Println("call ", s)
}

type Module2 struct {
	Name string
}

func (m *Module2) OnRecv(s string) {
	fmt.Println("call ", s)
}

func NewModule(id int, name string) ModuleBase {
	if id == 1 {
		return &Module1{Name: name}
	}
	return &Module2{Name: name}
}
