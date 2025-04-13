package bookkeepergo

import stdlog "log"

var log logger = &stdLogger{}

type logger interface {
	Info(string, ...interface{})
	Debug(string, ...interface{})
	Error(string, ...interface{})
	Warn(string, ...interface{})
}

type stdLogger struct {
}

func (l *stdLogger) Info(format string, args ...interface{}) {
	stdlog.Printf("INFO: "+format, args...)
}

func (l *stdLogger) Debug(format string, args ...interface{}) {
	stdlog.Printf("DEBUG: "+format, args...)
}

func (l *stdLogger) Error(format string, args ...interface{}) {
	stdlog.Printf("ERROR: "+format, args...)
}

func (l *stdLogger) Warn(format string, args ...interface{}) {
	stdlog.Printf("WARNING: "+format, args...)
}
