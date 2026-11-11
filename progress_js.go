package main

import (
	"syscall/js"

	"github.com/moodykhalif23/datacom/pipeline"
)

// The page gets rows and bytes read every 2 MB of input. A ticker would
// starve: with one thread and no preemption, the stages keep handing work
// to each other and a timer goroutine never runs.
func init() {
	liveHook = func(mon *pipeline.Monitor) {
		report := js.Global().Get("datacomProgress")
		if report.Type() != js.TypeFunction {
			return
		}
		var last int64
		mon.OnRead(func() {
			if b := mon.Bytes.Load(); b-last >= 2<<20 {
				last = b
				report.Invoke(mon.RowsIn.Load(), b)
			}
		})
	}
}
