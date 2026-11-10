package main

import "fmt"

type slotKind int

const (
	kindModel slotKind = iota
	kindMedia
	kindAudio
)

func (k slotKind) String() string {
	switch k {
	case kindModel:
		return "model"
	case kindMedia:
		return "media"
	case kindAudio:
		return "audio"
	}
	return fmt.Sprintf("kind(%d)", int(k))
}

func parseSlotKind(s string) (slotKind, error) {
	switch s {
	case "", "model":
		return kindModel, nil
	case "media":
		return kindMedia, nil
	case "audio":
		return kindAudio, nil
	}
	return 0, fmt.Errorf("unknown kind %q (model, media or audio)", s)
}
