//go:build unix

// benchrun runs a command n times and appends one TSV line with the median
// wall time, best wall time, CPU seconds and peak RSS to a results file.
//
//	benchrun -n 3 -o results.tsv -tool qsv -op profile -rows 1m [-stdout FILE] -- cmd args...
package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"
)

func main() {
	n := flag.Int("n", 3, "repetitions")
	out := flag.String("o", "results.tsv", "results file (appended)")
	tool := flag.String("tool", "", "tool label")
	op := flag.String("op", "", "operation label")
	rows := flag.String("rows", "", "dataset label")
	stdout := flag.String("stdout", os.DevNull, "where the command's stdout goes")
	timeout := flag.Duration("timeout", 20*time.Minute, "per-run timeout")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "no command")
		os.Exit(2)
	}
	var walls []float64
	var cpu, rss float64
	exit, note := 0, ""
	for i := 0; i < *n; i++ {
		f, err := os.Create(*stdout)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		var errb strings.Builder
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Stdout, cmd.Stderr = f, &errb
		start := time.Now()
		if err := cmd.Start(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(*timeout):
			cmd.Process.Kill()
			<-done
			note = "timeout"
		}
		wall := time.Since(start).Seconds()
		f.Close()
		walls = append(walls, wall)
		ps := cmd.ProcessState
		if ru, ok := ps.SysUsage().(*syscall.Rusage); ok {
			c := float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6 + float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
			if c > cpu {
				cpu = c
			}
			r := float64(ru.Maxrss) / 1024 // kilobytes on Linux
			if runtime.GOOS == "darwin" {
				r /= 1024 // bytes on macOS
			}
			if r > rss {
				rss = r
			}
		}
		if ps.ExitCode() != 0 {
			exit = ps.ExitCode()
			e := strings.TrimSpace(errb.String())
			if len(e) > 160 {
				e = e[:160]
			}
			note = strings.ReplaceAll(strings.ReplaceAll(e, "\n", " | "), "\t", " ")
			break
		}
	}
	sort.Float64s(walls)
	med := walls[len(walls)/2]
	line := fmt.Sprintf("%s\t%s\t%s\t%.2f\t%.2f\t%.2f\t%.0f\t%d\t%s\n", *tool, *op, *rows, med, walls[0], cpu, rss, exit, note)
	fh, err := os.OpenFile(*out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	fh.WriteString(line)
	fh.Close()
	fmt.Fprintf(os.Stderr, "%-8s %-14s %-4s median %7.2fs  cpu %7.2fs  rss %6.0f MB  exit %d %s\n", *tool, *op, *rows, med, cpu, rss, exit, note)
}
