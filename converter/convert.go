package converter

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/go-graphite/go-whisper"
	"github.com/it-novum/rrd2whisper/logging"
	"github.com/it-novum/rrd2whisper/oitcdb"
	"github.com/it-novum/rrd2whisper/rrdpath"
	perfdata "github.com/jabdr/nagios-perfdata"
)

var illegalCharactersRegexp = regexp.MustCompile(`[^a-zA-Z^0-9\\-\\.]`)

func replaceIllegalCharacters(s string) string {
	return illegalCharactersRegexp.ReplaceAllString(s, "_")
}

var whisperRetention whisper.Retentions

// SetRetention must be called before first conversion
func SetRetention(retention string) error {
	var err error
	whisperRetention, err = whisper.ParseRetentionDefs(retention)
	if err != nil {
		return fmt.Errorf("could not parse whisper retention: %s", err)
	}
	return nil
}

// Converter converts rrd files to whisper
type Converter struct {
	Merge       bool
	DeleteRRD   bool
	Destination string
	ArchivePath string
	TempPath    string
	OITC        *oitcdb.OITC
}

func (cvt *Converter) checkPerfdata(servicename string) ([]string, error) {
	if cvt.OITC != nil {
		perfStr, err := cvt.OITC.FetchPerfdata(servicename)
		if err != nil {
			return nil, err
		}
		if perfStr != "" {
			logging.Log("service perfdata in db \"%s\" -> \"%s\"", servicename, perfStr)
			pfdatas, err := perfdata.ParsePerfdata(perfStr)
			if err != nil {
				logging.Log("service %s has invalid perfdata in db: %s", servicename, err)
				return nil, nil
			}
			result := make([]string, len(pfdatas))
			for i, pf := range pfdatas {
				result[i] = pf.Label
			}
			return result, nil
		}
	}
	return nil, nil
}

type convertSource struct {
	Label               string
	DestinationFilename string
	TempFilename        string
	ArchiveFilename     string
	Whisper             *whisper.Whisper
}

func newConvertSource(label, destdir, tmpdir, archivedir string) (*convertSource, error) {
	var err error
	newLabel := replaceIllegalCharacters(label)
	cs := convertSource{
		Label:               newLabel,
		TempFilename:        fmt.Sprintf("%s/%s.wsp", tmpdir, newLabel),
		DestinationFilename: fmt.Sprintf("%s/%s.wsp", destdir, newLabel),
	}
	if archivedir == "" {
		cs.ArchiveFilename = ""
	} else {
		cs.ArchiveFilename = fmt.Sprintf("%s/%s.wsp", archivedir, newLabel)
	}
	cs.Whisper, err = whisper.Create(cs.TempFilename, whisperRetention, whisper.Average, 0.5)
	if err != nil {
		return nil, fmt.Errorf("could not create whisper file: %s", err)
	}

	return &cs, nil
}

func (cs *convertSource) merge(lastUpdate int) error {
	if _, err := os.Stat(cs.DestinationFilename); !os.IsNotExist(err) {
		logging.Log("Merge whisper file \"%s\" with \"%s\"", cs.TempFilename, cs.DestinationFilename)
		oldws, err := whisper.Open(cs.DestinationFilename)
		if err != nil {
			return fmt.Errorf("Could not open old whisper databaase: %s", err)
		}
		defer oldws.Close()
		timeSeries, err := oldws.Fetch(lastUpdate, int(time.Now().Unix()))
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
		logging.Log("Successfully merged \"%s\"", cs.TempFilename)
	}
	return nil
}

func (cs *convertSource) archive() error {
	if _, err := os.Stat(cs.DestinationFilename); !os.IsNotExist(err) {
		if cs.ArchiveFilename != "" {
			logging.Log("Move old whisper to archive \"%s\" -> \"%s\"", cs.DestinationFilename, cs.ArchiveFilename)
			if err := os.MkdirAll(filepath.Dir(cs.ArchiveFilename), 0755); err != nil {
				return fmt.Errorf("could not create directory for old whisper file archive: %s", err)
			}
			if err := os.Rename(cs.DestinationFilename, cs.ArchiveFilename); err != nil {
				return fmt.Errorf("could not move old whisper file to archive: %s", err)
			}
		}
	}
	return nil
}

func (cs *convertSource) mergeAndArchive(lastUpdate int) error {
	if err := cs.merge(lastUpdate); err != nil {
		return err
	}

	if err := cs.archive(); err != nil {
		return err
	}

	return nil
}

// Convert an rrd file to whisper files
func (cvt *Converter) Convert(ctx context.Context, rrdSet *rrdpath.RrdSet) error {
	dbLabels, err := cvt.checkPerfdata(rrdSet.Servicename)
	if err != nil {
		return err
	}
	if dbLabels != nil {
		if len(dbLabels) != len(rrdSet.Datasources) {
			return fmt.Errorf("invalid number of perfdata values db %d != xml %d", len(dbLabels), len(rrdSet.Datasources))
		}
		rrdSet.Datasources = dbLabels
	}

	destdir := fmt.Sprintf("%s/%s/%s", cvt.Destination, rrdSet.Hostname, rrdSet.Servicename)
	archivedir := ""
	if cvt.ArchivePath != "" {
		archivedir = fmt.Sprintf("%s/%s/%s", cvt.ArchivePath, rrdSet.Hostname, rrdSet.Servicename)
	}
	tmpdir, err := ioutil.TempDir(cvt.TempPath, "rrd2whisper")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpdir)

	dumperHelper, err := NewRrdDumperHelper(ctx, rrdSet.RrdPath)
	if err != nil {
		return err
	}

	sources := make([]*convertSource, len(rrdSet.Datasources))
	for i, label := range rrdSet.Datasources {
		sources[i], err = newConvertSource(label, destdir, tmpdir, archivedir)
		if err != nil {
			return err
		}
	}
	lastUpdate := sources[0].Whisper.StartTime()

	cache := newTimeSeriesCache(sources, 100000)
	for row := range dumperHelper.Results() {
		ts := int(row.Time.Unix())
		lastUpdate = ts
		if err := cache.addRow(ts, row.Values); err != nil {
			return err
		}
	}
	if err := cache.flush(); err != nil {
		return err
	}

	// Check if canceld while dumping
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if cvt.Merge {
		for _, source := range sources {
			source.mergeAndArchive(lastUpdate)
		}
	} else if cvt.ArchivePath != "" {
		for _, source := range sources {
			source.archive()
		}
	}

	if err := cache.close(); err != nil {
		return err
	}

	if err = os.MkdirAll(destdir, 0755); err != nil {
		return fmt.Errorf("could not create destination directory: %s", err)
	}

	for _, cs := range sources {
		if err = os.Rename(cs.TempFilename, cs.DestinationFilename); err != nil {
			return fmt.Errorf("could not move wsp file to destination directory: %s", err)
		}
	}

	var deleteError error = nil

	if cvt.DeleteRRD {
		deleteError = os.Remove(rrdSet.RrdPath)
	}

	err = rrdSet.Done()
	if err != nil {
		return err
	}
	if deleteError != nil {
		return deleteError
	}

	return nil
}
