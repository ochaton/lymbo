package lymbo

import "errors"

// Common errors returned by the lymbo package.
var (
	ErrHandlerNotFound         = errors.New("handler not found")
	ErrLimitInvalid            = errors.New("limit is invalid")
	ErrTicketIDEmpty           = errors.New("ticket ID is empty")
	ErrTicketIDInvalid         = errors.New("ticket ID is invalid")
	ErrTicketNil               = errors.New("ticket cannot be nil")
	ErrTicketNotFound          = errors.New("ticket not found")
	ErrInvalidStatusTransition = errors.New("invalid status transition")
)
