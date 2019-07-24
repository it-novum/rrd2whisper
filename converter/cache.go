package converter

import (
	"github.com/go-graphite/go-whisper"
)

type timeSeriesCache struct {
	values    []*whisper.TimeSeriesPoint
	sources   int
	positions []int
	size      int
}

func newTimeSeriesCache(sources, cacheSize int) *timeSeriesCache {
	tsc := new(timeSeriesCache)
	tsc.sources = sources
	tsc.size = cacheSize
	tsc.values = make([]*whisper.TimeSeriesPoint, cacheSize*sources)
	tsc.positions = make([]int, sources)
	for i := 0; i < sources; i++ {
		tsc.positions[i] = i * cacheSize
	}
	return tsc
}

func (tsc *timeSeriesCache) addRow(ts int, values []float64) (full bool) {
	full = false
	for i, value := range values {
		pos := tsc.positions[i]
		tsc.values[pos] = &whisper.TimeSeriesPoint{
			Time:  ts,
			Value: value,
		}
		tsc.positions[i] = pos + 1
	}
	full = tsc.positions[0] == tsc.size
	return full
}

func (tsc *timeSeriesCache) rowForSource(source int) []*whisper.TimeSeriesPoint {
	startPos := source * tsc.size
	endPos := tsc.positions[source]
	return tsc.values[startPos:endPos]
}
