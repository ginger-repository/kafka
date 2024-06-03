package producer

import (
	"github.com/ginger-core/errors"
)

const (
	ErrorCodeNilWriter = iota + 1
	ErrorCodeTimedOut
	ErrorCodeNotEnabled
)

var writerNotInitializedError = errors.Forbidden().
	WithCode(ErrorCodeNilWriter).
	WithDesc("writer not initialized")

var isNotEnabledError = errors.Forbidden().
	WithCode(ErrorCodeNotEnabled).
	WithDesc("is not enabled")
