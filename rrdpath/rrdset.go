package rrdpath

import (
	"fmt"
	"os"
	"time"
)

// RrdSet holds all data that is needed to process the rrd file
type RrdSet struct {
	RrdPath string
	Datasources []string
	Hostname string
	Servicename string
	Updated bool
	Time time.Time
	okPath string
}

// NewRrdSet abstracts the xml information to something usefull
func NewRrdSet(xml *XMLNagios) *RrdSet {
	ds := make([]string, len(xml.Datasources))
	for i, c := range xml.Datasources {
		ds[i] = c.Name
	}
	return &RrdSet{
		okPath: xml.Path[:len(xml.Path)-4] + ".ok",
		RrdPath: xml.Path[:len(xml.Path)-4] + ".rrd",
		Hostname: xml.Hostname,
		Servicename: xml.Servicename,
		Time: time.Unix(xml.TimeT, 0),
		Datasources: ds,
		Updated: xml.RrdTxt == "successful updated",
	}
}

// Todo checks the age and if the .ok file exists
func (rrdSet *RrdSet) Todo() bool {
	if _, err := os.Stat(rrdSet.okPath); os.IsNotExist(err) {
		return true
	}
	return false
}

// TooOld checks the last time updated of xml file
func (rrdSet *RrdSet) TooOld(oldest time.Time) bool {
	if rrdSet.Time.After(oldest) {
		return false
	}
	return true
}

// Done creates the .ok file
func (rrdSet *RrdSet) Done() error {
	okFl, err := os.OpenFile(rrdSet.okPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("could not create %s file: %s", rrdSet.okPath, err)
	}
	okFl.Close()
	return nil
}

// Workdata counts all found xml files with there states
type Workdata struct {
	RrdSets []*RrdSet
	TooOld uint64
	Corrupt uint64
	BrokenXML uint64
	Todo uint64
	Total uint64
}

// NewWorkdata processes all found xml files for stats
func NewWorkdata(rrdPath *RrdPath, oldest time.Time, limit int) (*Workdata, error) {
	rrdSets := make([]*RrdSet, 0)
	workdata := &Workdata{
		TooOld: 0,
		Corrupt: 0,
		BrokenXML: 0,
		Total: 0,
	}

	for xml := range rrdPath.Results() {
		rrd := NewRrdSet(xml)
		workdata.Total++
		if !rrd.Updated {
			workdata.Corrupt++
		} else if rrd.TooOld(oldest) {
			workdata.TooOld++
		} else if rrd.Todo() {
			rrdSets = append(rrdSets, rrd)
			workdata.Todo++
		}
	}
	err := rrdPath.Error()
	if err != nil {
		return nil, err
	}

	workdata.BrokenXML = rrdPath.BrokenXML()
	if limit <= 0 {
		workdata.RrdSets = rrdSets
	} else {
		workdata.RrdSets = rrdSets[:limit]
	}
	return workdata, nil
}
