// Package window maps wall-clock time onto fixed, epoch-aligned evaluation
// windows. Because windows are aligned to the Unix epoch, every pod buckets a
// timestamp into the same window without any coordination.
package window

import (
	"strconv"
	"time"
)

// ID identifies a window by its start time in Unix seconds.
type ID int64

// Of returns the window that contains t.
func Of(t time.Time, size time.Duration) ID {
	sec := int64(size / time.Second)
	u := t.Unix()
	// Floor division so times before the epoch still align correctly.
	start := u - ((u%sec)+sec)%sec
	return ID(start)
}

// Start is the first instant of the window.
func (id ID) Start() time.Time { return time.Unix(int64(id), 0).UTC() }

// End is the first instant after the window.
func (id ID) End(size time.Duration) time.Time { return id.Start().Add(size) }

// Prev is the window immediately before id.
func (id ID) Prev(size time.Duration) ID { return id - ID(size/time.Second) }

// Next is the window immediately after id.
func (id ID) Next(size time.Duration) ID { return id + ID(size/time.Second) }

func (id ID) String() string { return strconv.FormatInt(int64(id), 10) }
