package main

import (
	"math"
	"strconv"
	"strings"
	"fmt"
	"regexp"
)

type Perfdata struct {
	label string
	value float64
	uom string
	min float64
	max float64
	warning float64
	critical float64
}

var perfdataSplitRegex = regexp.MustCompile(`(('[^']+')?[^\s]+)`)
var perfdataLabelRegex = regexp.MustCompile(`^(('[^']+')?([^='])*)`)
var perfdataValueRegex = regexp.MustCompile(`^(\d+[\.,]?\d*)`)

func perfdataParseValue(valueStr string) (*Perfdata, error) {
	var err error
	pd := &Perfdata{
		label: "",
		value: math.NaN(),
		uom: "",
		warning: math.NaN(),
		critical: math.NaN(),
		min: math.NaN(),
		max: math.NaN(),
	}
	data := strings.Split(valueStr, ";")
	dataLen := len(data)
	if dataLen < 1 || dataLen > 5{
		return nil, fmt.Errorf("invalid perfdata value: %s", valueStr)
	}
	// 'label'=value[UOM];[warn];[crit];[min];[max]
	if dataLen == 5 && data[4] != "" {
		if pd.max, err = strconv.ParseFloat(data[4], 64); err != nil {
			return nil, fmt.Errorf("could not parse max value \"%s\": %s", data[4], err)
		}
	}
	if dataLen >= 4 && data[3] != "" {
		if pd.min, err = strconv.ParseFloat(data[3], 64); err != nil {
			return nil, fmt.Errorf("could not parse min value \"%s\": %s", data[3], err)
		}
	}
	if dataLen >= 3 && data[2] != "" {
		if pd.critical, err = strconv.ParseFloat(data[2], 64); err != nil {
			return nil, fmt.Errorf("could not parse critical value \"%s\": %s", data[2], err)
		}
	}
	if dataLen >= 2 && data[1] != "" {
		if pd.warning, err = strconv.ParseFloat(data[1], 64); err != nil {
			return nil, fmt.Errorf("could not parse warning value \"%s\": %s", data[1], err)
		}
	}
	pd.label = perfdataLabelRegex.FindString(data[0])
	if pd.label == "" {
		return nil, fmt.Errorf("invalid label: %s", data[0])
	}
	if len(pd.label) == len(data[0]) {
		return nil, fmt.Errorf("no value found: %s", data[0])
	}
	if data[0][len(pd.label)] != '=' {
		return nil, fmt.Errorf("invalid format %s", data[0])
	}
	valueWithUnit := data[0][len(pd.label)+1:]
	rawval := perfdataValueRegex.FindString(valueWithUnit)
	if rawval == "" {
		return nil, fmt.Errorf("invalid decimal in: %s", data[0])
	}
	if pd.value, err = strconv.ParseFloat(rawval, 64); err != nil {
		return nil, fmt.Errorf("could not parse value \"%s\": %s", data[0], err)
	}

	if len(rawval) == len(valueWithUnit) {
		pd.uom = ""
	} else {
		pd.uom = valueWithUnit[len(rawval):]
	}

	return pd, nil
}

func parsePerfdata(perfdata string) ([]*Perfdata, error) {
	var err error
	valueStrings := perfdataSplitRegex.FindAllString(perfdata, -1)
	if len(valueStrings) == 0 {
		return nil, fmt.Errorf("could not split perfdata: %s", perfdata)
	}
	values := make([]*Perfdata, len(valueStrings))
	for i, valueStr := range valueStrings {
		if values[i], err = perfdataParseValue(valueStr); err != nil {
			return nil, fmt.Errorf("could not parse perfdata: %s", err)
		}
	}
	return values, nil
}
