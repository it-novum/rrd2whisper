package main

import (
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

var perfdataSplitRegex *regexp.Regexp
var perfdataLabelRegex *regexp.Regexp
var perfdataValueRegex *regexp.Regexp

func init() {
	perfdataSplitRegex = regexp.MustCompile(`(('[^']+')?[^\s]+)`)
	perfdataLabelRegex = regexp.MustCompile(`^(('[^']+')?("[^"]+")?([^=])*)`)
	perfdataValueRegex = regexp.MustCompile(`^(\d+[\.,]?\d*)`)
}

func perfdataParseValue(valueStr string) (*Perfdata, error) {
	var err error
	pd := new(Perfdata)
	data := strings.Split(valueStr, ";")
	dataLen := len(data)
	if dataLen < 1 || dataLen > 5{
		return nil, fmt.Errorf("Invalid perfdata value: %s", valueStr)
	}
	// 'label'=value[UOM];[warn];[crit];[min];[max]
	if dataLen == 5 {
		if pd.max, err = strconv.ParseFloat(data[4], 64); err != nil {
			return nil, fmt.Errorf("Could not parse max value \"%s\": %s", data[4], err)
		}
	}
	if dataLen >= 4 {
		if pd.min, err = strconv.ParseFloat(data[3], 64); err != nil {
			return nil, fmt.Errorf("Could not parse min value \"%s\": %s", data[3], err)
		}
	}
	if dataLen >= 3 {
		if pd.critical, err = strconv.ParseFloat(data[2], 64); err != nil {
			return nil, fmt.Errorf("Could not parse critical value \"%s\": %s", data[2], err)
		}
	}
	if dataLen >= 2 {
		if pd.warning, err = strconv.ParseFloat(data[1], 64); err != nil {
			return nil, fmt.Errorf("Could not parse warning value \"%s\": %s", data[1], err)
		}
	}
	pd.label = perfdataLabelRegex.FindString(data[0])
	if pd.label == "" {
		return nil, fmt.Errorf("Invalid label value: %s", data[0])
	}
	valueWithUnit := data[0][len(pd.label)+1:]
	rawval := perfdataValueRegex.FindString(valueWithUnit)
	if rawval == "" {
		return nil, fmt.Errorf("Invalid decimal in: %s", data[0])
	}
	if pd.value, err = strconv.ParseFloat(rawval, 64); err != nil {
		return nil, fmt.Errorf("Could not parse value \"%s\": %s", data[0], err)
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
		return nil, fmt.Errorf("Could not split perfdata into values: %s", perfdata)
	}
	values := make([]*Perfdata, len(valueStrings))
	for i, valueStr := range valueStrings {
		if values[i], err = perfdataParseValue(valueStr); err != nil {
			return nil, fmt.Errorf("could not parse perfdata value: %s", err)
		}
	}
	return values, nil
}
