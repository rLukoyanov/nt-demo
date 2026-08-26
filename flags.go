package main

import (
	"fmt"
	"time"
)

// durationFlag implements flag.Value for time.Duration.
type durationFlag time.Duration

func (d *durationFlag) String() string { return time.Duration(*d).String() }

func (d *durationFlag) Set(s string) error {
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = durationFlag(v)
	return nil
}
