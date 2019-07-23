package main

import (
	"fmt"
	"math"
	"testing"
)


var testPerfdataSetOk = []struct{
	in string
	out []*Perfdata
}{
	{
		in: "users=2;3;7;0",
		out: []*Perfdata{
			&Perfdata{
				label: "users",
				uom: "",
				value: 2,
				warning: 3,
				critical: 7,
				min: 0,
				max: math.NaN(),
			},
		},
	},
	{
		in: "load1=0.050;7.000;10.000;0; load5=0.040;6.000;7.000;0; load15=0.010;5.000;6.000;0;",
		out: []*Perfdata{
			&Perfdata{
				label: "load1",
				uom: "",
				value: 0.050,
				warning: 7.000,
				critical: 10.000,
				min: 0,
				max: math.NaN(),
			},
			&Perfdata{
				label: "load5",
				uom: "",
				value: 0.040,
				warning: 6.000,
				critical: 7.000,
				min: 0,
				max: math.NaN(),
			},
			&Perfdata{
				label: "load15",
				uom: "",
				value: 0.010,
				warning: 5.000,
				critical: 6.000,
				min: 0,
				max: math.NaN(),
			},
		},
	},
	{
		in: "'users'=2%;3;7;0",
		out: []*Perfdata{
			&Perfdata{
				label: "'users'",
				uom: "%",
				value: 2,
				warning: 3,
				critical: 7,
				min: 0,
				max: math.NaN(),
			},
		},
	},
	{
		in: "users=2;;;;",
		out: []*Perfdata{
			&Perfdata{
				label: "users",
				uom: "",
				value: 2,
				warning: math.NaN(),
				critical: math.NaN(),
				min: math.NaN(),
				max: math.NaN(),
			},
		},
	},
}

var testPerfdataSetFail = []struct{
	in string
	err string
	out []*Perfdata
}{
	{
		in: "users'=2%;3;7;0",
		out: []*Perfdata{
			&Perfdata{
				label: "users'",
				uom: "%",
				value: 2,
				warning: 3,
				critical: 7,
				min: 0,
				max: math.NaN(),
			},
		},
		err: "could not parse perfdata: invalid format users'=2%",
	},
	{
		in: "users2%;3;7;0",
		out: []*Perfdata{
			&Perfdata{
				label: "users",
				uom: "",
				value: 2,
				warning: 3,
				critical: 7,
				min: 0,
				max: math.NaN(),
			},
		},
		err: "could not parse perfdata: no value found: users2%",
	},
}

func testPerfdata(in string, out []*Perfdata) error {
	result, err := parsePerfdata(in)
	if err != nil {
		return err
	}
	if len(result) != len(out) {
		return fmt.Errorf("number of parsed perfdata %d != expected %d", len(result), len(out))
	}
	for i := 0; i < len(result); i++ {
		if out[i].label != result[i].label {
			return fmt.Errorf("label is not equal")
		}
		if out[i].uom != result[i].uom {
			return fmt.Errorf("uom is not equal")
		}
		if !(out[i].warning == result[i].warning || (math.IsNaN(out[i].warning) && math.IsNaN(result[i].warning))) {
			return fmt.Errorf("warning is not equal")
		}
		if !(out[i].critical == result[i].critical || (math.IsNaN(out[i].critical) && math.IsNaN(result[i].critical))) {
			return fmt.Errorf("critical is not equal")
		}
		if !(out[i].min == result[i].min || (math.IsNaN(out[i].min) && math.IsNaN(result[i].min))) {
			return fmt.Errorf("min is not equal")
		}
		if !(out[i].max == result[i].max || (math.IsNaN(out[i].max) && math.IsNaN(result[i].max))) {
			return fmt.Errorf("max is not equal")
		}
	}
	return nil
}

func TestParsePerfdata(t *testing.T) {
	for _, pftest := range testPerfdataSetOk {
		if err := testPerfdata(pftest.in, pftest.out); err != nil {
			t.Errorf("in: %s err: %s", pftest.in, err)
		}
	}
	for _, pftest := range testPerfdataSetFail {
		err := testPerfdata(pftest.in, pftest.out)
		if err == nil {
			t.Errorf("in: %s expected error \"%s\" but it passed", pftest.in, pftest.err)
		} else {
			if err.Error() != pftest.err {
				t.Errorf("in: %s expected error \"%s\" but got \"%s\"", pftest.in, pftest.err, err)
			}
		}
	}
}
