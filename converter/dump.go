package converter

import (
	"context"
	"fmt"
	"github.com/jabdr/rrd"
	"math"
)

// RrdDumperHelper wrapps arround rrd.RrdDumper to provide a cancable worker
type RrdDumperHelper struct {
	ctx     context.Context
	dumper  *rrd.RrdDumper
	results chan *rrd.RrdDumpRow
}

// NewRrdDumperHelper creates the background thread for rrd.RrdDumper
func NewRrdDumperHelper(ctx context.Context, path string) (*RrdDumperHelper, error) {
	var err error
	rdh := &RrdDumperHelper{
		ctx:     ctx,
		results: make(chan *rrd.RrdDumpRow, 1000),
	}
	rdh.dumper, err = rrd.NewDumper(path, "AVERAGE")
	if err != nil {
		return nil, fmt.Errorf("could not open rrd file: %s", err)
	}

	go rdh.work()

	return rdh, nil
}

func (rdh *RrdDumperHelper) work() {
	defer rdh.dumper.Free()
	defer close(rdh.results)
	for row := rdh.dumper.Next(); row != nil; row = rdh.dumper.Next() {
		hasVal := false
		for _, val := range row.Values {
			if !math.IsNaN(val) {
				hasVal = true
				break
			}
		}
		if hasVal {
			select {
			case <-rdh.ctx.Done():
				return
			case rdh.results <- row:
			}
		} else {
			select {
			case <-rdh.ctx.Done():
				return
			default:
			}
		}
	}
}

// Results returns a channel with the rows of the rrd file
func (rdh *RrdDumperHelper) Results() <-chan *rrd.RrdDumpRow {
	return rdh.results
}
