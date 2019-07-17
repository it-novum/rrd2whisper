package main

import (
	"os"
	"time"
)

func checkCurruptRrd(xmlFiles []*XmlNagios) []*XmlNagios {
	todoFiles := make([]*XmlNagios, 0, len(xmlFiles))

	for _, fl := range xmlFiles {
		if fl.RrdTxt == "successful updated" {
			todoFiles = append(todoFiles, fl)
		}
	}

	return todoFiles
}

func checkOkFiles(xmlFiles []*XmlNagios) []*XmlNagios {
	todoFiles := make([]*XmlNagios, 0, len(xmlFiles))

	for _, fl := range xmlFiles {
		if _, err := os.Stat(fl.OkPath); os.IsNotExist(err) {
			todoFiles = append(todoFiles, fl)
		}
	}

	return todoFiles
}

func checkAgeFiles(xmlFiles []*XmlNagios, oldest int64) []*XmlNagios {
	todoFiles := make([]*XmlNagios, 0, len(xmlFiles))

	for _, fl := range xmlFiles {
		if _, err := os.Stat(fl.OkPath); os.IsNotExist(err) {
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
	brokenXMLCount int
	xmlFiles       []*XmlNagios
}

func gatherWorkdata(cli *Cli) *Workdata {
	var err error
	workdata := new(Workdata)

	xmlFiles := make([]*XmlNagios, 0)
	workdata.brokenXMLCount, err = walkSourceTree(cli.sourceDirectory, func(xmlNagios *XmlNagios, path string) {
		xmlFiles = append(xmlFiles, xmlNagios)
	})
	if err != nil {
		logAndPrintf("Warning: There were a problem search and parsing for xml files: %s", err)
	}
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
