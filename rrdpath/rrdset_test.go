package rrdpath

import (
	"context"
	"github.com/it-novum/rrd2whisper/testsuite"
	"github.com/jabdr/nagios-perfdata"
	"testing"
	"time"
)

func Test(t *testing.T) {
	ts := testsuite.Prepare()
	defer ts.Shutdown()
	pf, err := perfdata.ParsePerfdata("label1=0%;0;0;0; 'label2'=34")
	if err != nil {
		panic(err)
	}
	testsuite.CreateRrd(ts.Source, "abc", "abc", pf, time.Now().Add(-testsuite.DAY).Add(-testsuite.DAY*2), time.Now().Add(-testsuite.DAY*2), false)

	rrdPath := Walk(context.Background(), ts.Source)
	var maxAge time.Time
	workdata, err := NewWorkdata(rrdPath, maxAge, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(workdata.RrdSets) != 1 {
		t.Fatalf("Found %d, but should only be 1", len(workdata.RrdSets))
	}
	if len(workdata.RrdSets[0].Datasources) != 2 {
		t.Fatalf("Found %d datasources instead of 2", len(workdata.RrdSets[0].Datasources))
	}
	for _, rrdSet := range workdata.RrdSets {
		if err := rrdSet.Done(); err != nil {
			t.Errorf("RrdSet.Done(): %s", err)
		}
	}
}

func TestBrokenXML(t *testing.T) {
	ts := testsuite.Prepare()
	defer ts.Shutdown()
	pf, err := perfdata.ParsePerfdata("label1=0%;0;0;0; 'label2'=34")
	if err != nil {
		panic(err)
	}
	testsuite.CreateRrd(ts.Source, "abc", "abc", pf, time.Now().Add(-testsuite.DAY).Add(-testsuite.DAY*2), time.Now().Add(-testsuite.DAY*2), true)

	rrdPath := Walk(context.Background(), ts.Source)
	var maxAge time.Time
	wdata, err := NewWorkdata(rrdPath, maxAge, 0)
	if err != nil {
		t.Fatal(err)
	}
	if wdata.BrokenXML != 0 {
		t.Error("BrokenXML count is 0, but should be 1")
	}
}

func TestCancel(t *testing.T) {
	ts := testsuite.Prepare()
	defer ts.Shutdown()
	pf, err := perfdata.ParsePerfdata("label1=0%;0;0;0; 'label2'=34")
	if err != nil {
		panic(err)
	}
	testsuite.CreateRrd(ts.Source, "abc", "abc", pf, time.Now().Add(-testsuite.DAY).Add(-testsuite.DAY*2), time.Now().Add(-testsuite.DAY*2), false)

	ctx, cancel := context.WithCancel(context.Background())
	rrdPath := Walk(ctx, ts.Source)
	var maxAge time.Time
	cancel()
	_, err = NewWorkdata(rrdPath, maxAge, 0)
	if err == nil || err.Error() != "context canceled" {
		t.Fatalf("err is not context canceled: %s", err)
	}
}
