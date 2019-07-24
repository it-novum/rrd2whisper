package main

import (
	"github.com/it-novum/rrd2whisper/rrdpath"
	"github.com/it-novum/rrd2whisper/oitcdb"
	"github.com/jabdr/nagios-perfdata"
	"strings"
	"log"
	"math"
	"fmt"
	"github.com/go-graphite/go-whisper"
	"github.com/jabdr/rrd"
	"io/ioutil"
	"os"
	"time"
)

type convertSource struct {
	WspName             string
	DestinationFilename string
	TempFilename        string
	Whisper             *whisper.Whisper
}

type dataCache struct {
	values    []*whisper.TimeSeriesPoint
	sources   int
	positions []int
	size      int
}

func newDataCache(sources, cacheSize int) *dataCache {
	dc := new(dataCache)
	dc.sources = sources
	dc.size = cacheSize
	dc.values = make([]*whisper.TimeSeriesPoint, cacheSize*sources)
	dc.positions = make([]int, sources)
	for i := 0; i < sources; i++ {
		dc.positions[i] = i * cacheSize
	}
	return dc
}

func (dc *dataCache) addRow(ts int, values []float64) (full bool) {
	full = false
	for i, value := range values {
		pos := dc.positions[i]
		dc.values[pos] = &whisper.TimeSeriesPoint{
			Time:  ts,
			Value: value,
		}
		dc.positions[i] = pos + 1
	}
	full = dc.positions[0] == dc.size
	return full
}

func (dc *dataCache) rowForSource(source int) []*whisper.TimeSeriesPoint {
	startPos := source * dc.size
	endPos := dc.positions[source]
	return dc.values[startPos:endPos]
}

func convertRrd(rrdSet *rrdpath.RrdSet, dest, oldWhisperDir string, mergeExisting bool, oitc *oitcdb.OITC) error {
	var perfdatas []*perfdata.Perfdata

	if oitc != nil {
		// TODO: should be part of rrdpath
		log.Printf("check for perfdata in db")
		perfStr, err := oitc.FetchPerfdata(rrdSet.Servicename)
		if err != nil {
			return err
		}
		log.Printf("got perfdata %s", perfStr)
		if perfStr != "" {
			perfdatas, err = perfdata.ParsePerfdata(perfStr)
			if err != nil {
				log.Printf("service %s has invalid perfdata in db: %s\n", rrdSet.Servicename, err)
			} else {
				if len(perfdatas) != len(rrdSet.Datasources) {
					return fmt.Errorf("invalid number of perfdata values db %d != xml %d", len(perfdatas), len(rrdSet.Datasources))
				}
			}
		}
	}

	dumper, err := rrd.NewDumper(rrdSet.RrdPath, "AVERAGE")
	if err != nil {
		return fmt.Errorf("could not dump rrd file: %s", err)
	}

	destdir := fmt.Sprintf("%s/%s/%s", dest, rrdSet.Hostname, rrdSet.Servicename)

	tmpdir, err := ioutil.TempDir("/tmp", "rrd2whisper")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpdir)

	var logstr strings.Builder
	pflen := 0
	if perfdatas != nil {
		pflen = len(perfdatas)
	}
	logstr.WriteString(fmt.Sprintf("%s has %d datasources and %d perfdata values", rrdSet.RrdPath, len(rrdSet.Datasources), pflen))
	convertSources := make([]convertSource, len(rrdSet.Datasources))
	for i, ds := range rrdSet.Datasources {
		rawName := ds
		if perfdatas != nil {
			rawName = perfdatas[i].Label
		}
		convertSources[i].WspName = replaceIllegalCharacters(rawName)
		convertSources[i].TempFilename = fmt.Sprintf("%s/%s.wsp", tmpdir, convertSources[i].WspName)
		convertSources[i].DestinationFilename = fmt.Sprintf("%s/%s.wsp", destdir, convertSources[i].WspName)
		convertSources[i].Whisper, err = whisper.Create(convertSources[i].TempFilename, whisperRetention, whisper.Average, 0.5)
		if err != nil {
			return fmt.Errorf("Could not create whisper file: %s", err)
		}
		logstr.WriteString(fmt.Sprintf(" DS%d: `%s`", i, rawName))
	}
	logstr.WriteString("\n")
	log.Print(logstr.String())
	numSources := len(convertSources)
	cache := newDataCache(numSources, 100000)
	flushCache := func() error {
		for i := 0; i < numSources; i++ {
			err = convertSources[i].Whisper.UpdateMany(cache.rowForSource(i))
			if err != nil {
				return fmt.Errorf("Could not update whisper file: %s", err)
			}
		}
		return nil
	}

	startTime := convertSources[0].Whisper.StartTime()
	lastTime := startTime

	rowChan := make(chan *rrd.RrdDumpRow, 10000)

	go func() {
		for row := dumper.Next(); row != nil; row = dumper.Next() {
			rowChan <- row
		}
		close(rowChan)
	}()

	for row := range rowChan {
		ts := int(row.Time.Unix())
		lastTime = ts
		full := cache.addRow(ts, row.Values)
		if full {
			if err = flushCache(); err != nil {
				return err
			}
			cache = newDataCache(numSources, 100000)
		}
	}
	if err = flushCache(); err != nil {
		return err
	}

	for _, cs := range convertSources {
		if _, err := os.Stat(cs.DestinationFilename); !os.IsNotExist(err) {
			if mergeExisting {
				oldws, err := whisper.Open(cs.DestinationFilename)
				if err != nil {
					return fmt.Errorf("Could not open old whisper databaase: %s", err)
				}
				timeSeries, err := oldws.Fetch(lastTime+60, int(time.Now().Unix()))
				if err != nil {
					return fmt.Errorf("Could not fetch data from old whisper database: %s", err)
				}
				pts := timeSeries.PointPointers()
				cleanPoints := make([]*whisper.TimeSeriesPoint, 0, len(pts))
				for _, pt := range pts {
					if !math.IsNaN(pt.Value) {
						cleanPoints = append(cleanPoints, pt)
					}
				}
				if err = cs.Whisper.UpdateMany(cleanPoints); err != nil {
					return fmt.Errorf("could not merge data from old whisper file: %s", err)
				}
				oldws.Close()
			}
			if oldWhisperDir != "" {
				pathArchiveWhisper := fmt.Sprintf("%s/%s/%s", oldWhisperDir, rrdSet.Hostname, rrdSet.Servicename)
				err = os.MkdirAll(pathArchiveWhisper, 0755)
				if err != nil {
					// TODO: not break
					return fmt.Errorf("Could not create path for old whisper file archive: %s", err)
				}
				os.Rename(cs.DestinationFilename, fmt.Sprintf("%s/%s.wsp", pathArchiveWhisper, cs.WspName))
			}
		}
	}

	for _, cs := range convertSources {
		err = cs.Whisper.Close()
		if err != nil {
			return fmt.Errorf("Could not write whisper file: %s", err)
		}
	}

	if err = os.MkdirAll(destdir, 0755); err != nil {
		return fmt.Errorf("Could not create destination directory: %s", err)
	}

	for _, cs := range convertSources {
		if err = os.Rename(cs.TempFilename, cs.DestinationFilename); err != nil {
			return fmt.Errorf("Could not move wsp file to destination directory: %s", err)
		}
	}

	if err = rrdSet.Done(); err != nil {
		return err
	}

	return nil
}
