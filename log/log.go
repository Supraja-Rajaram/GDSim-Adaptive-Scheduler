package log

import (
	"fmt"
	"io"
	"log"
)

type Level int

const (
	FATAL Level = iota
	ERROR
	WARN
	INFO
	DEBUG
)

type Context struct {
	id string
}

type manager struct {
	level    Level
	contexts map[string]bool
}

var logger manager

func init() {
	logger = manager{
		level:    ERROR,
		contexts: make(map[string]bool),
	}
}

func (m manager) fatalf(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}

func (m manager) printf(level Level, id string, format string, v ...interface{}) {
	if level <= m.level && m.contexts[id] {
		log.Printf(format, v...)
	}
}

func SetLevel(level Level) {
	logger.level = level
}

func EnableContext(id string) {
	logger.contexts[id] = true
}

func SetFlags(flag int) {
	log.SetFlags(flag)
}

func SetOutput(writer io.Writer) {
	log.SetOutput(writer)
}

func New(id string) Context {
	logger.contexts[id] = false
	return Context{id}
}

func (c Context) Fatalf(format string, v ...interface{}) {
	compound := fmt.Sprintf("FATAL: %s: %v", c.id, format)
	logger.fatalf(compound, v...)
}

func (c Context) Errorf(format string, v ...interface{}) {
	compound := fmt.Sprintf("ERROR: %s: %v", c.id, format)
	logger.printf(ERROR, c.id, compound, v...)
}

func (c Context) Warnf(format string, v ...interface{}) {
	compound := fmt.Sprintf("WARN: %s: %v", c.id, format)
	logger.printf(WARN, c.id, compound, v...)
}

func (c Context) Infof(format string, v ...interface{}) {
	compound := fmt.Sprintf("INFO: %s: %v", c.id, format)
	logger.printf(INFO, c.id, compound, v...)
}

func (c Context) Debugf(format string, v ...interface{}) {
	compound := fmt.Sprintf("DEBUG: %s: %v", c.id, format)
	logger.printf(DEBUG, c.id, compound, v...)
}
