package logging

import (
	"fmt"
	"log"
)

type PrintLogFunc func (message string)

var PrintDisplayLog PrintLogFunc = func(message string) {
	fmt.Println(message)
}

func Log(format string, v ...interface{}) {
	log.Println(fmt.Sprintf(format, v...))
}

func LogV(v ...interface{}) {
	log.Println(fmt.Sprint(v...))
}

func LogDisplay(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	PrintDisplayLog(msg)
	log.Println(msg)
}

func LogDisplayV(v ...interface{}) {
	msg := fmt.Sprint(v...)
	PrintDisplayLog(msg)
	log.Println(msg)
}

func LogFatal(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	PrintDisplayLog(msg)
	log.Fatalln(msg)
}

func LogFatalV(v ...interface{}) {
	msg := fmt.Sprint(v...)
	PrintDisplayLog(msg)
	log.Fatalln(msg)
}
