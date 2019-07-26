package converter

import (
	"context"
	"github.com/it-novum/rrd2whisper/testsuite"
	"github.com/jabdr/nagios-perfdata"
	"testing"
	"time"
)

func TestRrdDumperHelperCancel(t *testing.T) {
	ts := testsuite.Prepare()
	defer ts.Shutdown()

	pf, err := perfdata.ParsePerfdata("label1=0%;0;0;0; 'label2'=34")
	if err != nil {
		panic(err)
	}
	testData := testsuite.CreateRrd(ts.Source, "abc", "abc", pf, time.Now().Add(-testsuite.DAY), time.Now(), false)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dumper, err := NewRrdDumperHelper(ctx, testData.Path)
	if err != nil {
		t.Fatal(err)
	}
	counter := 0
	for range dumper.Results() {
		counter++
	}

	if counter > 1 {
		t.Errorf("cancel did not work as expected: found %d results", counter)
	}
}
