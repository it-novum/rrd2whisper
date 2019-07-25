package logging

import (
	"fmt"
	"log"
)

func Log(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func LogV(v ...interface{}) {
	log.Print(v...)
}

func LogDisplay(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	log.Printf(format, v...)
}

func LogDisplayV(v ...interface{}) {
	fmt.Print(v...)
	log.Print(v...)
}

func LogFatal(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	log.Fatalf(format, v...)
}

func LogFatalV(v ...interface{}) {
	fmt.Print(v...)
	log.Fatal(v...)
}
