package lymbo

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/ochaton/lymbo/status"
)

type TicketPayloadMarshaler interface {
	MarshalPayload() ([]byte, error)
}

type TicketPayloadUnmarshaler interface {
	UnmarshalPayload([]byte) error
}

type TicketPayload interface {
	TicketPayloadMarshaler
	TicketPayloadUnmarshaler
}

// TicketId is a unique identifier for a ticket.
type TicketId string

func (t TicketId) String() string {
	return string(t)
}

// Ticket represents a job to be processed.
type Ticket struct {
	ID          TicketId
	Status      status.Status
	Runat       time.Time       // Time when the ticket should be processed
	Nice        int             // Priority value (lower = higher priority)
	Type        string          // Ticket type identifier for routing
	Ctime       time.Time       // Creation time
	Mtime       *time.Time      // Last modification time
	Attempts    int             // Number of processing attempts
	Payload     json.RawMessage // Serialized payload data
	ErrorReason json.RawMessage // Serialized error information if processing failed
}

var (
	// ErrTidEmpty is returned when a ticket ID is empty.
	ErrTidEmpty = errors.New("ticket ID cannot be empty")
	// ErrTypeEmpty is returned when a ticket type is empty.
	ErrTypeEmpty = errors.New("ticket type cannot be empty")
)

// DefaultNice is the default priority value for new tickets.
const DefaultNice = 512

// NewTicket creates a new ticket with the given ID and type.
// Returns an error if tid or typ is empty.
func NewTicket(tid TicketId, typ string) (*Ticket, error) {
	if tid == "" {
		return nil, ErrTidEmpty
	}
	if typ == "" {
		return nil, ErrTypeEmpty
	}

	now := time.Now()
	return &Ticket{
		ID:          tid,
		Runat:       now,
		Status:      status.Pending,
		Nice:        DefaultNice,
		Type:        typ,
		Ctime:       now,
		Mtime:       nil,
		Attempts:    0,
		Payload:     nil,
		ErrorReason: nil,
	}, nil
}

// WithPayload sets the payload for the ticket and returns the ticket.
func (t *Ticket) WithPayload(payload any) *Ticket {
	r := toDefaultPayloadMarshaller(payload)
	t.Payload, _ = r.MarshalPayload()
	return t
}

// WithNice sets the priority for the ticket and returns the ticket.
func (t *Ticket) WithNice(nice int) *Ticket {
	t.Nice = nice
	return t
}

// WithRunat sets the run time for the ticket and returns the ticket.
func (t *Ticket) WithRunat(runat time.Time) *Ticket {
	t.Runat = runat
	return t
}
