package oitcdb

import (
	"github.com/go-ini/ini"
	"fmt"
)

func readMySQLINI(path string) (string, error) {
	cfg, err := ini.Load(path)
	if err != nil {
		return "", fmt.Errorf("could not read mysql ini file: %s", err)
	}
	sec := cfg.Section("client")
	if sec == nil {
		return "", fmt.Errorf("no client section in mysql ini file")
	}
	var (
		host     string
		port     string
		user     string
		password string
		database string
	)

	if host, err = readIniString(sec, "host"); err != nil {
		return "", err
	}
	if port, err = readIniString(sec, "port"); err != nil {
		port = "3306"
	}
	if user, err = readIniString(sec, "user"); err != nil {
		return "", err
	}
	if password, err = readIniString(sec, "password"); err != nil {
		return "", err
	}
	if database, err = readIniString(sec, "database"); err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", user, password, host, port, database), nil
}

func readIniString(sec *ini.Section, name string) (string, error) {
	k := sec.Key(name)
	if k == nil {
		return "", fmt.Errorf("Could not find %s in client section of mysql ini", name)
	}
	return k.String(), nil
}