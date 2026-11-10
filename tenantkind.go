package main

import "fmt"

type tenantKind int

const (
	kindModel tenantKind = iota
	kindMedia
	kindAudio
)

func (k tenantKind) String() string {
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

func parseTenantKind(s string) (tenantKind, error) {
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
