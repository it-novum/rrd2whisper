package main

import (
	"testing"
)

func TestParsePerfdata(t *testing.T) {
	t.Run("basics", func(t *testing.T) {
		perfdata, err := parsePerfdata(`'label1'=123%;23;40.23;50;2`)
		if err != nil {
			t.Error(err)
		}
		if len(perfdata) != 1 {
			t.Errorf("Unexpected number of perfdata in result: %d", len(perfdata))
		}
		if perfdata[0].label != `'label1'` {
			t.Errorf("Unexpected label %s != %s", `'label1'`, perfdata[0].label)
		}
		if perfdata[0].uom != "%" {
			t.Errorf("Unexpected uom %s != %s", "%", perfdata[0].uom)
		}
		if perfdata[0].warning != float64(23) {
			t.Errorf("Unexpected warning %s != %s", "%", perfdata[0].uom)
		}
		if perfdata[0].critical != float64(40.23) {
			t.Errorf("Unexpected critical %s != %s", "%", perfdata[0].uom)
		}
		if perfdata[0].min != float64(50) {
			t.Errorf("Unexpected min %s != %s", "%", perfdata[0].uom)
		}
		if perfdata[0].max != float64(2) {
			t.Errorf("Unexpected max %s != %s", "%", perfdata[0].uom)
		}
	})
	t.Run("multiple", func(t *testing.T) {
		perfdata, err := parsePerfdata(`'labe l1'=123%;23;40;50;2 'lab%e l3'=123%;23;40;50;2 label3=23`)
		if err != nil {
			t.Error(err)
		}
		if len(perfdata) != 3 {
			t.Errorf("Unexpected number of perfdata in result: %d", len(perfdata))
		}
	})
}
