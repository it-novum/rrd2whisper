package converter

import (
	"context"
	"github.com/it-novum/rrd2whisper/logging"
	"github.com/it-novum/rrd2whisper/rrdpath"
	"sync"
)

// RrdSetVisitor is called by the worker after a conversion
type RrdSetVisitor interface {
	// error will be set if there was an error converting the rrd
	Visit(*rrdpath.RrdSet, error)
}

// Worker helps to process a list of rrd files to whisper
type Worker struct {
	cvt       *Converter
	visitor   RrdSetVisitor
	jobs      chan *rrdpath.RrdSet
	ctx       context.Context
	rrdSets   []*rrdpath.RrdSet
	WaitGroup sync.WaitGroup
}

// NewWorker starts processing the rrd files
func NewWorker(ctx context.Context, rrdSets []*rrdpath.RrdSet, parallel int, cvt *Converter, visitor RrdSetVisitor) *Worker {
	w := Worker{
		ctx:     ctx,
		cvt:     cvt,
		visitor: visitor,
		jobs:    make(chan *rrdpath.RrdSet, parallel+1),
		rrdSets: rrdSets,
	}

	for i := 0; i < parallel; i++ {
		w.WaitGroup.Add(1)
		go w.work()
	}

	w.WaitGroup.Add(1)
	go w.iterate()

	return &w
}

func (w *Worker) work() {
	defer w.WaitGroup.Done()
	done := w.ctx.Done()
	for {
		select {
		case <-done:
			return
		case job, ok := <-w.jobs:
			if !ok {
				return
			}
			err := w.cvt.Convert(job)
			if err != nil {
				logging.Log("error: Could not convert rrd file %s: %s", job.RrdPath, err)
			} else {
				logging.Log("successfully converted %s to whisper", job.RrdPath)
			}
			w.visitor.Visit(job, err)
		}
	}
}

func (w *Worker) iterate() {
	defer w.WaitGroup.Done()
	defer close(w.jobs)
	for _, rrdSet := range w.rrdSets {
		select {
		case w.jobs <- rrdSet:
			continue
		case <-w.ctx.Done():
			return
		}
	}
}
