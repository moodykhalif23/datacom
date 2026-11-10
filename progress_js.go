package main

import (
	"syscall/js"
	"time"

	"github.com/moodykhalif23/datacom/pipeline"
)

func init() {
	liveHook = func(mon *pipeline.Monitor, stop <-chan struct{}) {
		report := js.Global().Get("datacomProgress")
		if report.Type() != js.TypeFunction {
			return
		}
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				report.Invoke(mon.RowsIn.Load(), mon.Bytes.Load())
			}
		}
	}
}
