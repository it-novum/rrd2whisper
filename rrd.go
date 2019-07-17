package main

import (
	"log"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

type XmlDatasource struct {
	Name string `xml:"NAME"`
}

type XmlNagios struct {
	XMLName     string `xml:"NAGIOS"`
	Hostname    string `xml:"NAGIOS_HOSTNAME"`
	Servicename string `xml:"NAGIOS_SERVICEDESC"`
	RrdTxt      string `xml:"RRD>TXT"`
	TimeT       int64  `xml:"NAGIOS_TIMET"`
	RrdPath     string
	OkPath      string
	Datasources []XmlDatasource `xml:"DATASOURCE"`
}

func parseRrdXML(path string) (*XmlNagios, error) {
	xmldata, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Could not read xml file: %s", err)
	}
	xmlstruct := new(XmlNagios)
	err = xml.Unmarshal(xmldata, xmlstruct)
	if err != nil {
		return nil, fmt.Errorf("Could not parse xml structure: %s", err)
	}

	xmlstruct.RrdPath = path[:len(path)-4] + ".rrd"
	xmlstruct.OkPath = path[:len(path)-4] + ".ok"

	return xmlstruct, nil
}

type rrdSourceFunc func(xmlNagios *XmlNagios, path string)

func walkSourceTree(path string, cb rrdSourceFunc) (int, error) {
	brokenXMLCount := 0
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".xml") {
			xmlNagios, err := parseRrdXML(path)
			if err != nil {
				brokenXMLCount++
				log.Printf("Could not read xml file: %s", err)
			} else {
				cb(xmlNagios, path)
			}
		}
		return nil
	})
	return brokenXMLCount, err
}
