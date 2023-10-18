package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

var signalCh chan os.Signal
var IsProfile bool

func InstallSignal(callBack func()) {
	signalCh = make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGPIPE, syscall.SIGABRT)
	procSignal(callBack)
}

func procSignal(callBack func()) {
	for {
		s := <-signalCh
		switch s {
		case syscall.SIGPIPE:
			continue
		case syscall.SIGTERM: //正常结束程序
			if callBack != nil {
				callBack()
			}
		default:
			fmt.Printf("got sig %v", signalCh)
			if callBack != nil {
				callBack()
			}
		}
	}
}
