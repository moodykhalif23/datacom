package pipeline

import (
	osexec "os/exec"

	"github.com/parquet-go/parquet-go"
)

// exec runs a command; tests use it to find the module cache.
func exec(name string, args ...string) (string, error) {
	out, err := osexec.Command(name, args...).Output()
	return string(out), err
}

func parquetInt64(v int64) parquet.Value { return parquet.Int64Value(v) }
