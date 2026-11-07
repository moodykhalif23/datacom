package pipeline

import (
	osexec "os/exec"

	"github.com/parquet-go/parquet-go"
)

// exec runs a command for tests that need the module cache location.
func exec(name string, args ...string) (string, error) {
	out, err := osexec.Command(name, args...).Output()
	return string(out), err
}

func parquetInt64(v int64) parquet.Value { return parquet.Int64Value(v) }
