package converter

import (
	"context"
	"sync"
	"time"

	"github.com/it-novum/rrd2whisper/logging"
	"github.com/it-novum/rrd2whisper/rrdpath"
)

// RrdSetVisitor is called by the worker after a conversion
type RrdSetVisitor interface {
	// error will be set if there was an error converting the rrd
	Visit(*rrdpath.RrdSet, time.Duration, error)
}

// Worker helps to process a list of rrd files to whisper
type Worker struct {
	cvt     *Converter
	visitor RrdSetVisitor
	jobs    chan *rrdpath.RrdSet
	ctx     context.Context
	rrdSets []*rrdpath.RrdSet
	wg      *sync.WaitGroup
	begin   time.Time
}

// NewWorker starts processing the rrd files
func NewWorker(ctx context.Context, wg *sync.WaitGroup, rrdSets []*rrdpath.RrdSet, parallel int, cvt *Converter, visitor RrdSetVisitor) *Worker {
	w := Worker{
		ctx:     ctx,
		cvt:     cvt,
		visitor: visitor,
		jobs:    make(chan *rrdpath.RrdSet, parallel+1),
		rrdSets: rrdSets,
		wg:      wg,
		begin:   time.Now(),
	}

	for i := 0; i < parallel; i++ {
		w.wg.Add(1)
		go w.work()
	}

	w.wg.Add(1)
	go w.iterate()

	return &w
}

func (w *Worker) work() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case job, ok := <-w.jobs:
			if !ok {
				return
			}
			err := w.cvt.Convert(w.ctx, job)
			if err != nil {
				logging.LogDisplay("error: Could not convert rrd file %s: %s", job.RrdPath, err)
			} else {
				logging.LogDisplay("successfully converted %s to whisper", job.RrdPath)
			}
			w.visitor.Visit(job, time.Since(w.begin), err)
		}
	}
}

func (w *Worker) iterate() {
	defer w.wg.Done()
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
