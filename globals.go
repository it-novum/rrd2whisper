package main

import (
	"fmt"
	"regexp"

	"github.com/go-graphite/go-whisper"
)

var illegalCharactersRegexp = regexp.MustCompile(`[^a-zA-Z^0-9\\-\\.]`)
var whisperRetention whisper.Retentions

func initGlobals(cli *Cli) error {
	var err error
	whisperRetention, err = whisper.ParseRetentionDefs(cli.retention)
	if err != nil {
		return fmt.Errorf("Could not parse whisper retention: %s", err)
	}
	return nil
}

func replaceIllegalCharacters(s string) string {
	return illegalCharactersRegexp.ReplaceAllString(s, "_")
}
