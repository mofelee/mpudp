package main

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
)

func startProfiles(prefix string) (func() error, error) {
	if prefix == "" {
		return func() error { return nil }, nil
	}
	if err := os.MkdirAll(filepath.Dir(prefix), 0700); err != nil {
		return nil, err
	}
	cpu, err := os.OpenFile(prefix+".cpu.pprof", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	if err := pprof.StartCPUProfile(cpu); err != nil {
		_ = cpu.Close()
		return nil, err
	}
	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
	return func() error {
		pprof.StopCPUProfile()
		var result error
		result = errors.Join(result, cpu.Close())
		runtime.SetMutexProfileFraction(0)
		runtime.SetBlockProfileRate(0)
		for _, name := range []string{"allocs", "heap", "mutex", "block"} {
			f, err := os.OpenFile(prefix+"."+name+".pprof", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				result = errors.Join(result, err)
				continue
			}
			result = errors.Join(result, pprof.Lookup(name).WriteTo(f, 0), f.Close())
		}
		return result
	}, nil
}
