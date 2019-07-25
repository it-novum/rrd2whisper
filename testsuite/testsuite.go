package testsuite

import (
	"text/template"
	"strings"
	"strconv"
	"math/rand"
	"fmt"
	"github.com/jabdr/nagios-perfdata"
	"github.com/jabdr/rrd"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

var illegalCharactersRegexp = regexp.MustCompile(`[^a-zA-Z^0-9\\-\\.]`)

func replaceIllegalCharacters(s string) string {
	return illegalCharactersRegexp.ReplaceAllString(s, "_")
}

type TestSuite struct {
	baseDir string
	Source  string
	Archive string
	Temp    string
}

type xmlData struct {
	perfdata.Perfdata
	RrdPath string
	XMLPath string
	LastUpdate int64
	Hostname string
	Servicename string
	Number int
}

func writeRrdXML(xmlpath, rrdpath, hostname, servicename string, pflist []*perfdata.Perfdata, lastUpdate int64, broken bool) {
	xmlBegin := `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><NAGIOS>`
	xmlDS := template.Must(template.New("datasource").Parse(`<DATASOURCE>
<TEMPLATE>cdd9ba25-a4d8-4261-a551-32164d4dde14</TEMPLATE>
<RRDFILE>{{.RrdPath}}</RRDFILE>
<RRD_STORAGE_TYPE>SINGLE</RRD_STORAGE_TYPE>
<RRD_HEARTBEAT>8460</RRD_HEARTBEAT>
<IS_MULTI>0</IS_MULTI>
<DS>{{.Number}}</DS>
<NAME>{{.Label}}</NAME>
<LABEL>{{.Label}}</LABEL>
<UNIT>{{.UOM}}</UNIT>
<ACT>{{.Value}}</ACT>
<WARN>{{.Warning}}</WARN>
<WARN_MIN></WARN_MIN>
<WARN_MAX></WARN_MAX>
<WARN_RANGE_TYPE></WARN_RANGE_TYPE>
<CRIT>{{.Critical}}</CRIT>
<CRIT_MIN></CRIT_MIN>
<CRIT_MAX></CRIT_MAX>
<CRIT_RANGE_TYPE></CRIT_RANGE_TYPE>
<MIN>{{.Min}}</MIN>
<MAX>{{.Max}}</MAX>
</DATASOURCE>
`))
	xmlend := template.Must(template.New("end").Parse(`<RRD>
<RC>1</RC>
<TXT>successful updated</TXT>
</RRD>
<NAGIOS_AUTH_HOSTNAME></NAGIOS_AUTH_HOSTNAME>
<NAGIOS_AUTH_SERVICEDESC></NAGIOS_AUTH_SERVICEDESC>
<NAGIOS_CHECK_COMMAND><![CDATA[cdd9ba25-a4d8-4261-a551-32164d4dde14!100.0,20%!500.0,60%]]></NAGIOS_CHECK_COMMAND>
<NAGIOS_DATATYPE>SERVICEPERFDATA</NAGIOS_DATATYPE>
<NAGIOS_DISP_HOSTNAME>{{.Hostname}}</NAGIOS_DISP_HOSTNAME>
<NAGIOS_DISP_SERVICEDESC>{{.Servicename}}</NAGIOS_DISP_SERVICEDESC>
<NAGIOS_HOSTNAME>{{.Hostname}}</NAGIOS_HOSTNAME>
<NAGIOS_HOSTSTATE></NAGIOS_HOSTSTATE>
<NAGIOS_HOSTSTATETYPE></NAGIOS_HOSTSTATETYPE>
<NAGIOS_MULTI_PARENT></NAGIOS_MULTI_PARENT>
<NAGIOS_PERFDATA></NAGIOS_PERFDATA>
<NAGIOS_RRDFILE>{{.RrdPath}}</NAGIOS_RRDFILE>
<NAGIOS_SERVICECHECKCOMMAND><![CDATA[cdd9ba25-a4d8-4261-a551-32164d4dde14!100.0,20%!500.0,60%]]></NAGIOS_SERVICECHECKCOMMAND>
<NAGIOS_SERVICEDESC>{{.Servicename}}</NAGIOS_SERVICEDESC>
<NAGIOS_SERVICEPERFDATA></NAGIOS_SERVICEPERFDATA>
<NAGIOS_SERVICESTATE>OK</NAGIOS_SERVICESTATE>
<NAGIOS_SERVICESTATETYPE>HARD</NAGIOS_SERVICESTATETYPE>
<NAGIOS_TIMET>{{.LastUpdate}}</NAGIOS_TIMET>
<NAGIOS_XMLFILE>{{.XMLPath}}</NAGIOS_XMLFILE>
<XML>
<VERSION>4</VERSION>
</XML>
</NAGIOS>`))

	var xmlOut strings.Builder
	xmlOut.WriteString(xmlBegin)
	
	for i, pf := range pflist {
		inData := xmlData{
			Perfdata: *pf,
			RrdPath: rrdpath,
			XMLPath: xmlpath,
			Hostname: hostname,
			Servicename: servicename,
			LastUpdate: lastUpdate,
			Number: i,
		}
		xmlDS.Execute(&xmlOut, inData)
	}
	inData := xmlData{
		Perfdata: *pflist[len(pflist)-1],
		RrdPath: rrdpath,
		XMLPath: xmlpath,
		Hostname: hostname,
		Servicename: servicename,
		LastUpdate: lastUpdate,
		Number: 0,
	}
	xmlend.Execute(&xmlOut, inData)

	if broken {
		xmlOut.WriteString("</dff")
	}

	if err := ioutil.WriteFile(xmlpath, []byte(xmlOut.String()), 0644); err != nil {
		panic(err)
	}
}

func generateRandomTimeSeriesRrd(unixFrom, unixTo int64, pflist []*perfdata.Perfdata) [][]string {
	rand.Seed(time.Now().Unix())
	minutes := (unixTo - unixFrom) / 60
	values := make([][]string, minutes-1)
	for _, pf := range pflist {
		if math.IsNaN(pf.Min) {
			pf.Min = 0.0
		}
		if math.IsNaN(pf.Max) {
			pf.Max = 1000.0
		}
	}

	
	for i := int64(1); i < minutes; i++ {
		row := make([]string, len(pflist)+1)
		row[0] = strconv.FormatInt(unixFrom + (i * 60), 10)
		for c, pf := range pflist {
			row [c+1] = strconv.FormatFloat(((pf.Max-pf.Min) * rand.Float64() + pf.Min), 'f', 2, 64)
		}
		values[i-1] = row
	}

	return values
}

func CreateRrd(path string, hostname string, servicename string, pflist []*perfdata.Perfdata, from time.Time, to time.Time, brokenXML bool) {
	var (
		rrdPath = fmt.Sprintf("%s/%s/%s.rrd", path, hostname, servicename)
		xmlPath = fmt.Sprintf("%s/%s/%s.xml", path, hostname, servicename)
	)
	os.MkdirAll(filepath.Dir(rrdPath), 0755)

	rrdFile := rrd.NewCreator(rrdPath, from, 60)
	rrdFile.RRA("AVERAGE", 0.5, 1, 576000)
	rrdFile.RRA("MAX", 0.5, 1, 576000)
	rrdFile.RRA("MIN", 0.5, 1, 576000)
	for i, pf := range pflist {
		var (
			compute string
			min     = "U"
			max     = "U"
		)
		switch {
		case pf.UOM == "c":
			compute = "COUNTER"
		case pf.UOM == "d":
			compute = "DERIVE"
		default:
			compute = "GAUGE"
		}
		if !math.IsNaN(pf.Min) {
			min = fmt.Sprintf("%f", pf.Min)
		}
		if !math.IsNaN(pf.Max) {
			max = fmt.Sprintf("%f", pf.Max)
		}
		rrdFile.DS(fmt.Sprintf("%d", i), compute, "8460", min, max)
	}
	if err := rrdFile.Create(true); err != nil {
		panic(err)
	}
	rrdUpd := rrd.NewUpdater(rrdPath)
	if rrdUpd == nil {
		panic("could not update rrd")
	}
	perfValues := generateRandomTimeSeriesRrd(from.Unix(), to.Unix(), pflist)
	for _, values := range perfValues {
		rrdUpd.Cache(strings.Join(values, ":"))
	}
	if err := rrdUpd.Update(); err != nil {
		panic(err)
	}

	writeRrdXML(xmlPath, rrdPath, hostname, servicename, pflist, to.Unix(), brokenXML)
}

func Prepare() *TestSuite {
	baseDir, err := ioutil.TempDir("/tmp", "testrrd2whisper")
	if err != nil {
		panic(err)
	}
	ts := &TestSuite{
		baseDir: baseDir,
		Source:  fmt.Sprintf("%s/%s", baseDir, "source"),
		Archive: fmt.Sprintf("%s/%s", baseDir, "archive"),
		Temp:    fmt.Sprintf("%s/%s", baseDir, "temp"),
	}
	if err := os.MkdirAll(ts.Source, 0755); err != nil {
		panic(err)
	}

	return ts
}

func (ts *TestSuite) Shutdown() {
	os.RemoveAll(ts.baseDir)
}
