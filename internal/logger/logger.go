package logger

import "github.com/ginger-core/log/logger"

type Logger interface {
	logger.Logger
	Printf(string, ...interface{})
}

type l struct {
	logger.Logger
}

func New(logger logger.Logger) Logger {
	l := &l{
		Logger: logger,
	}
	return l
}

func (l *l) Printf(fmt string, args ...interface{}) {
	l.Debugf(fmt, args...)
}
