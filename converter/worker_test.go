package converter

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/it-novum/rrd2whisper/oitcdb"
	"github.com/it-novum/rrd2whisper/rrdpath"
	"github.com/it-novum/rrd2whisper/testsuite"
	perfdata "github.com/jabdr/nagios-perfdata"
)

type testWorkerVisitor struct {
	errors  []error
	counter int
}

func (twv *testWorkerVisitor) Visit(_ *rrdpath.RrdSet, _ time.Duration, err error) {
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

	var wg sync.WaitGroup

	cvt := &Converter{Destination: ts.Destination, ArchivePath: ts.Archive, TempPath: ts.Temp, Merge: true, UUIDToPerfdata: make(oitcdb.UUIDToPerfdata), DeleteRRD: false}
	NewWorker(context.Background(), &wg, workdata.RrdSets, 1, cvt, vs)
	wg.Wait()
	if len(vs.errors) != 0 {
		for i := 0; i < len(vs.errors); i++ {
			t.Error(vs.errors[i])
		}
	}
}

func TestWorkerDelete(t *testing.T) {
	ts := testsuite.Prepare()
	defer ts.Shutdown()

	SetRetention("60s:365d")

	pf, err := perfdata.ParsePerfdata("label1=0%;0;0;0; 'labe l2'=34")
	if err != nil {
		panic(err)
	}

	var oldest time.Time // == 0

	testData := testsuite.CreateRrd(ts.Source, "host1", "service1", pf, time.Now().Add(-testsuite.DAY), time.Now(), false)
	if _, err := os.Stat(testData.Path); os.IsNotExist(err) {
		t.Error("rrd file doesn't exist before walk! internal test error")
	}

	rrdPath := rrdpath.Walk(context.Background(), ts.Source)
	workdata, err := rrdpath.NewWorkdata(rrdPath, oldest, 0)
	if err != nil {
		t.Fatal(err)
	}

	vs := &testWorkerVisitor{
		errors: make([]error, 0),
	}

	var wg sync.WaitGroup

	cvt := &Converter{Destination: ts.Destination, ArchivePath: ts.Archive, TempPath: ts.Temp, Merge: true, UUIDToPerfdata: make(oitcdb.UUIDToPerfdata), DeleteRRD: true}
	NewWorker(context.Background(), &wg, workdata.RrdSets, 1, cvt, vs)
	wg.Wait()
	if len(vs.errors) != 0 {
		for i := 0; i < len(vs.errors); i++ {
			t.Error(vs.errors[i])
		}
	}

	if _, err := os.Stat(testData.Path); !os.IsNotExist(err) {
		t.Error("rrd file still exists")
	}
}

func TestWorkerNoDelete(t *testing.T) {
	ts := testsuite.Prepare()
	defer ts.Shutdown()

	SetRetention("60s:365d")

	pf, err := perfdata.ParsePerfdata("label1=0%;0;0;0; 'labe l2'=34")
	if err != nil {
		panic(err)
	}

	var oldest time.Time // == 0

	testData := testsuite.CreateRrd(ts.Source, "host1", "service1", pf, time.Now().Add(-testsuite.DAY), time.Now(), false)
	if _, err := os.Stat(testData.Path); os.IsNotExist(err) {
		t.Error("rrd file doesn't exist before walk! internal test error")
	}

	rrdPath := rrdpath.Walk(context.Background(), ts.Source)
	workdata, err := rrdpath.NewWorkdata(rrdPath, oldest, 0)
	if err != nil {
		t.Fatal(err)
	}

	vs := &testWorkerVisitor{
		errors: make([]error, 0),
	}

	var wg sync.WaitGroup

	cvt := &Converter{Destination: ts.Destination, ArchivePath: ts.Archive, TempPath: ts.Temp, Merge: true, UUIDToPerfdata: make(oitcdb.UUIDToPerfdata), DeleteRRD: false}
	NewWorker(context.Background(), &wg, workdata.RrdSets, 1, cvt, vs)
	wg.Wait()
	if len(vs.errors) != 0 {
		for i := 0; i < len(vs.errors); i++ {
			t.Error(vs.errors[i])
		}
	}

	if _, err := os.Stat(testData.Path); os.IsNotExist(err) {
		t.Error("rrd doesn't exist anymore")
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

	var wg sync.WaitGroup

	cvt := &Converter{Destination: ts.Destination, ArchivePath: ts.Archive, TempPath: ts.Temp, Merge: true, UUIDToPerfdata: make(oitcdb.UUIDToPerfdata), DeleteRRD: false}
	cancel()
	NewWorker(ctx, &wg, workdata.RrdSets, 1, cvt, vs)
	wg.Wait()
	if len(vs.errors) != 0 {
		t.Errorf("Expected no error, but got %d:", len(vs.errors))
		for i := 0; i < len(vs.errors); i++ {
			t.Error(vs.errors[i])
		}
		return
	}
}

func TestWorkerEmpty(t *testing.T) {
	ts := testsuite.Prepare()
	defer ts.Shutdown()

	SetRetention("60s:365d")

	var oldest time.Time // == 0

	rrdPath := rrdpath.Walk(context.Background(), ts.Source)
	workdata, err := rrdpath.NewWorkdata(rrdPath, oldest, 0)
	if err != nil {
		t.Fatal(err)
	}

	vs := &testWorkerVisitor{
		errors: make([]error, 0),
	}

	var wg sync.WaitGroup

	cvt := &Converter{Destination: ts.Destination, ArchivePath: ts.Archive, TempPath: ts.Temp, Merge: true, UUIDToPerfdata: make(oitcdb.UUIDToPerfdata), DeleteRRD: false}
	NewWorker(context.Background(), &wg, workdata.RrdSets, 1, cvt, vs)
	wg.Wait()
}
