package main

import (
	"context"
	"github.com/it-novum/rrd2whisper/rrdpath"
	"os"
	"time"
)

func checkCurruptRrd(xmlFiles []*rrdpath.XMLNagios) []*rrdpath.XMLNagios {
	todoFiles := make([]*rrdpath.XMLNagios, 0, len(xmlFiles))

	for _, fl := range xmlFiles {
		if fl.RrdTxt == "successful updated" {
			todoFiles = append(todoFiles, fl)
		}
	}

	return todoFiles
}

func checkOkFiles(xmlFiles []*rrdpath.XMLNagios) []*rrdpath.XMLNagios {
	todoFiles := make([]*rrdpath.XMLNagios, 0, len(xmlFiles))

	for _, fl := range xmlFiles {
		if _, err := os.Stat(fl.OkPath); os.IsNotExist(err) {
			todoFiles = append(todoFiles, fl)
		}
	}

	return todoFiles
}

func checkAgeFiles(xmlFiles []*rrdpath.XMLNagios, oldest int64) []*rrdpath.XMLNagios {
	todoFiles := make([]*rrdpath.XMLNagios, 0, len(xmlFiles))

	for _, fl := range xmlFiles {
		if fl.TimeT >= oldest {
			todoFiles = append(todoFiles, fl)
		}
	}

	return todoFiles
}

type Workdata struct {
	foundTotal     int
	foundTodo      int
	tooOld         int
	corrupt        int
	finalTodo      int
	brokenXMLCount uint64
	xmlFiles       []*rrdpath.XMLNagios
}

func gatherWorkdata(cli *Cli) *Workdata {
	var err error
	workdata := new(Workdata)

	xmlFiles := make([]*rrdpath.XMLNagios, 0)

	ctx := context.Background()
	walker := rrdpath.Walk(ctx, cli.sourceDirectory)

	for xmlNagios := range walker.Results() {
		xmlFiles = append(xmlFiles, xmlNagios)
	}
	err = walker.Error()
	if err != nil {
		logAndPrintf("Warning: There were a problem search and parsing for xml files: %s", err)
	}
	workdata.brokenXMLCount = walker.BrokenXML()
	workdata.foundTotal = len(xmlFiles)

	withoutCorrupt := checkCurruptRrd(xmlFiles)
	workdata.corrupt = len(xmlFiles) - len(withoutCorrupt)
	if !cli.includeCorrupt {
		xmlFiles = withoutCorrupt
	}

	xmlFiles = checkOkFiles(xmlFiles)
	workdata.foundTodo = len(xmlFiles)

	oldest := time.Now().Add(time.Duration(-cli.maxAge) * time.Second).Unix()
	xmlFiles = checkAgeFiles(xmlFiles, oldest)
	workdata.tooOld = workdata.foundTodo - len(xmlFiles)
	workdata.foundTodo = len(xmlFiles)

	if cli.limit > 0 && len(xmlFiles) > cli.limit {
		xmlFiles = xmlFiles[0:cli.limit]
	}
	workdata.finalTodo = len(xmlFiles)
	workdata.xmlFiles = xmlFiles

	return workdata
}
