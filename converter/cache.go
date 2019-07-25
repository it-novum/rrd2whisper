package converter

import (
	"fmt"
	"github.com/go-graphite/go-whisper"
)

type timeSeriesCache struct {
	values    []*whisper.TimeSeriesPoint
	sources   []*convertSource
	positions []int
	size      int
}

func newTimeSeriesCache(sources []*convertSource, cacheSize int) *timeSeriesCache {
	tsc := new(timeSeriesCache)
	tsc.sources = sources
	tsc.size = cacheSize
	tsc.positions = make([]int, len(sources))
	tsc.reset()
	return tsc
}

func (tsc *timeSeriesCache) reset() {
	tsc.values = make([]*whisper.TimeSeriesPoint, tsc.size*len(tsc.sources))
	for i := 0; i < len(tsc.sources); i++ {
		tsc.positions[i] = i * tsc.size
	}
}

func (tsc *timeSeriesCache) addRow(ts int, values []float64) error {
	for i, value := range values {
		pos := tsc.positions[i]
		tsc.values[pos] = &whisper.TimeSeriesPoint{
			Time:  ts,
			Value: value,
		}
		tsc.positions[i] = pos + 1
	}
	if tsc.positions[0] == tsc.size {
		return tsc.flush()
	}
	return nil
}

func (tsc *timeSeriesCache) rowForSource(source int) []*whisper.TimeSeriesPoint {
	startPos := source * tsc.size
	endPos := tsc.positions[source]
	return tsc.values[startPos:endPos]
}

func (tsc *timeSeriesCache) flush() error {
	if tsc.positions[0] != 0 {
		for i, source := range tsc.sources {
			if err := source.Whisper.UpdateMany(tsc.rowForSource(i)); err != nil {
				return fmt.Errorf("could not update whisper file: %s", err)
			}
		}
		tsc.reset()
	}
	return nil
}

func (tsc *timeSeriesCache) close() error {
	tsc.flush()
	for _, source := range tsc.sources {
		if err := source.Whisper.Close(); err != nil {
			return err
		}
	}
	return nil
}
