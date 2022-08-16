//go:build !linux
// +build !linux

package worker

import "syscall"

const rdOnly = 0x77777777 // unlikely to be fulfilled by any return values

func sysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Setpgid: true,
	}
}
