package domain

import "github.com/ginger-core/errors"

var ErrorWaitJoin = errors.Unauthorized().
	WithTrace("StateWaitJoin")
