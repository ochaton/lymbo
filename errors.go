package lymbo

import "errors"

// Common errors returned by the lymbo package.
var (
	ErrHandlerNotFound         = errors.New("handler not found")
	ErrLimitInvalid            = errors.New("limit is invalid")
	ErrTicketIDEmpty           = errors.New("ticket ID is empty")
	ErrTicketIDInvalid         = errors.New("ticket ID is invalid")
	ErrTicketNotFound          = errors.New("ticket not found")
	ErrInvalidStatusTransition = errors.New("invalid status transition")
	ErrFinalizerInGroup        = errors.New("finalizer must not be a member of the group it finalizes")
	ErrTubesNotEnabled         = errors.New("tubes are not enabled on this kharon instance")
	ErrAlreadyRunning          = errors.New("kharon: Run is already in progress")
)
