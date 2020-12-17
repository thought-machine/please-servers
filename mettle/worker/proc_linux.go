package worker

import "syscall"

const rdOnly = syscall.MS_RDONLY

// On Linux and FreeBSD, the getrusage(2) man pages document
// that the resident set size is returned in kilobytes, though
// kernel sources indicate kibibytes are used.
const maximumResidentSetSizeUnit = 1024

func sysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGTERM,
		Setpgid:   true,
	}
}
