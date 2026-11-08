// Command lanelist prints the packages of one lane, one per line, as
// ./relative paths for go build, vet and test (scripts/check-lane.sh).
//
//	go run ./internal/lanes/cmd/lanelist <lane>
//
// With no lane it prints each lane and its package count.
package main

import (
	"fmt"
	"os"

	"github.com/yakugate/yakugate/internal/lanes"
)

var all = []lanes.Lane{lanes.ClientEdge, lanes.VendorEdge, lanes.ReceiptsRecords,
	lanes.Data, lanes.ConfigOps, lanes.Core, lanes.Shared}

func main() {
	g, err := lanes.Graph()
	if err != nil {
		fmt.Fprintln(os.Stderr, "lanelist:", err)
		os.Exit(1)
	}
	if len(os.Args) < 2 {
		for _, l := range all {
			fmt.Printf("%-17s %-14s %d\n", l, lanes.Owner[l], len(lanes.In(g, l)))
		}
		return
	}
	lane := lanes.Lane(os.Args[1])
	if _, ok := lanes.Owner[lane]; !ok {
		fmt.Fprintf(os.Stderr, "lanelist: no lane %q (lanes: %v)\n", lane, all)
		os.Exit(2)
	}
	for _, p := range lanes.In(g, lane) {
		fmt.Println("./" + p)
	}
}
