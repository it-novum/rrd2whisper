package converter

import (
	"context"
	"github.com/it-novum/rrd2whisper/rrdpath"
	"github.com/it-novum/rrd2whisper/testsuite"
	"github.com/jabdr/nagios-perfdata"
	"testing"
	"time"
)

type testWorkerVisitor struct {
	errors  []error
	counter int
}

func (twv *testWorkerVisitor) Visit(_ *rrdpath.RrdSet, err error) {
	twv.counter++
	if err != nil {
		twv.errors = append(twv.errors, err)
	}
}

func TestWorker(t *testing.T) {
	ts := testsuite.Prepare()
	defer ts.Shutdown()

	SetRetention("60s:365d")

	pf, err := perfdata.ParsePerfdata("label1=0%;0;0;0; 'labe l2'=34")
	if err != nil {
		panic(err)
	}

	var oldest time.Time // == 0

	testsuite.CreateRrd(ts.Source, "host1", "service1", pf, time.Now().Add(-testsuite.DAY), time.Now(), false)

	rrdPath := rrdpath.Walk(context.Background(), ts.Source)
	workdata, err := rrdpath.NewWorkdata(rrdPath, oldest, 0)
	if err != nil {
		t.Fatal(err)
	}

	vs := &testWorkerVisitor{
		errors: make([]error, 0),
	}

	cvt := NewConverter(context.Background(), ts.Destination, ts.Archive, ts.Temp, true, nil)
	worker := NewWorker(context.Background(), workdata.RrdSets, 1, cvt, vs)
	worker.WaitGroup.Wait()
	if len(vs.errors) != 0 {
		for i := 0; i < len(vs.errors); i++ {
			t.Error(vs.errors[i])
		}
	}
}

func TestWorkerCancel(t *testing.T) {
	ts := testsuite.Prepare()
	defer ts.Shutdown()

	SetRetention("60s:365d")

	pf, err := perfdata.ParsePerfdata("label1=0%;0;0;0; 'labe l2'=34")
	if err != nil {
		panic(err)
	}

	var oldest time.Time // == 0

	testsuite.CreateRrd(ts.Source, "host1", "service1", pf, time.Now().Add(-testsuite.DAY*2), time.Now(), false)

	ctx, cancel := context.WithCancel(context.Background())

	rrdPath := rrdpath.Walk(ctx, ts.Source)
	workdata, err := rrdpath.NewWorkdata(rrdPath, oldest, 0)
	if err != nil {
		t.Fatal(err)
	}

	vs := &testWorkerVisitor{
		errors: make([]error, 0),
	}

	cvt := NewConverter(ctx, ts.Destination, ts.Archive, ts.Temp, true, nil)
	cancel()
	worker := NewWorker(ctx, workdata.RrdSets, 1, cvt, vs)
	worker.WaitGroup.Wait()
	if len(vs.errors) != 1 {
		t.Errorf("Expected 1 error with context canceled, but got %d:", len(vs.errors))
		for i := 0; i < len(vs.errors); i++ {
			t.Error(vs.errors[i])
		}
		return
	}
	if vs.errors[0].Error() != "context canceled" {
		t.Errorf("error message is not context canceled, maybe cancelation failed: %s", vs.errors[0])
	}
}
