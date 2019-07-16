package main

import (
	"fmt"
	"regexp"

	"github.com/go-graphite/go-whisper"
)
var illegalCharactersRegexp *regexp.Regexp
var whisperRetention whisper.Retentions

func initGlobals(cli *Cli) error {
	var err error
	illegalCharactersRegexp, err = regexp.Compile("[^a-zA-Z^0-9\\-\\.]")
	if err != nil {
		panic("Internal regexp library error")
	}
	whisperRetention, err = whisper.ParseRetentionDefs(cli.retention)
	if err != nil {
		return fmt.Errorf("Could not parse whisper retention: %s", err)
	}
	return nil
}

func replaceIllegalCharacters(s string) string {
	return illegalCharactersRegexp.ReplaceAllString(s, "_")
}
