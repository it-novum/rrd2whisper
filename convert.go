package main

import (
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
	SourceFilename      string
	Whisper             *whisper.Whisper
}


type dataCache struct {
	values []*whisper.TimeSeriesPoint
	sources int
	positions []int
	size int
}

func newDataCache(sources, cacheSize int) *dataCache {
	dc := new(dataCache)
	dc.sources = sources
	dc.size = cacheSize
	dc.values = make([]*whisper.TimeSeriesPoint, cacheSize * sources)
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
			Time: ts,
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

func convertRrd(xml *XmlNagios, dest, oldWhisperDir string, mergeExisting bool) error {
	dumper, err := rrd.NewDumper(xml.RrdPath, "AVERAGE")
	if err != nil {
		return fmt.Errorf("Could not dump rrd file: %s", err)
	}

	destdir := fmt.Sprintf("%s/%s/%s", dest, xml.Hostname, xml.Servicename)

	tmpdir, err := ioutil.TempDir("/tmp", "rrd2whisper")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpdir)

	convertSources := make([]convertSource, len(xml.Datasources))
	for i, ds := range xml.Datasources {
		convertSources[i].WspName = replaceIllegalCharacters(ds.Name)
		convertSources[i].SourceFilename = fmt.Sprintf("%s/%s.wsp", tmpdir, convertSources[i].WspName)
		convertSources[i].DestinationFilename = fmt.Sprintf("%s/%s.wsp", destdir, convertSources[i].WspName)
		convertSources[i].Whisper, err = whisper.Create(convertSources[i].SourceFilename, whisperRetention, whisper.Average, 0.5)
		if err != nil {
			return fmt.Errorf("Could not create whisper file: %s", err)
		}
	}
	numSources := len(convertSources)
	cache := newDataCache(numSources, 10000)

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
			for i := 0; i < numSources; i++ {
				err = convertSources[i].Whisper.UpdateMany(cache.rowForSource(i))
				if err != nil {
					return fmt.Errorf("Could not update whisper file: %s", err)
				}
			}
			cache = newDataCache(numSources, 10000)
		}
	}

	for _, cs := range convertSources {
		if _, err := os.Stat(cs.DestinationFilename); !os.IsNotExist(err) {
			if mergeExisting {
				oldws, err := whisper.Open(cs.DestinationFilename)
				if err != nil {
					// TODO: Not break here
					return fmt.Errorf("Could not open old whisper databaase: %s", err)
				}
				timeSeries, err := oldws.Fetch(lastTime, int(time.Now().Unix()))
				if err != nil {
					// TODO: Not break here
					return fmt.Errorf("Could not fetch data from old whisper database: %s", err)
				}
				for _, point := range timeSeries.Points() {
					cs.Whisper.Update(point.Value, point.Time)
				}
				oldws.Close()
			}
			if oldWhisperDir != "" {
				pathArchiveWhisper := fmt.Sprintf("%s/%s/%s", oldWhisperDir, xml.Hostname, xml.Servicename)
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

	err = os.MkdirAll(destdir, 0755)
	if err != nil {
		return fmt.Errorf("Could not create destination directory: %s", err)
	}

	for _, ds := range xml.Datasources {
		wspname := replaceIllegalCharacters(ds.Name)
		err = os.Rename(fmt.Sprintf("%s/%s.wsp", tmpdir, wspname), fmt.Sprintf("%s/%s.wsp", destdir, wspname))
		if err != nil {
			return fmt.Errorf("Could not move wsp file to destination directory: %s", err)
		}
	}

	okFl, err := os.OpenFile(xml.OkPath, os.O_CREATE | os.O_APPEND | os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("Could not create %s file: %s", xml.OkPath, err)
	}
	okFl.Close()

	return nil
}
