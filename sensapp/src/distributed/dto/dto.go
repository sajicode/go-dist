package dto

import (
	"encoding/gob"
	"time"
)

type SensorMessage struct {
	Name      string
	Value     float64
	Timestamp time.Time
}

func init() {
	// * register type so gob package knows how to work with it when we call on the type
	gob.Register(SensorMessage{})
}
