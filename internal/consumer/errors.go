package consumer

import (
	"github.com/ginger-core/errors"
)

const (
	ErrorCodeNilReader = iota + 1
	ErrorCodeTimedOut
	ErrorCodeNotEnabled
	ErrorCodeNotStarted
)

var readerNotInitializedError = errors.Forbidden().
	WithCode(ErrorCodeNilReader).
	WithDesc("reader not initialized")

var isNotEnabledError = errors.Forbidden().
	WithCode(ErrorCodeNotEnabled).
	WithDesc("is not enabled")

var isNotStartedError = errors.Forbidden().
	WithCode(ErrorCodeNotStarted).
	WithDesc("is not enabled")
