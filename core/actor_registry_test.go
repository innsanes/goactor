package core_test

import (
	"goactor/core"
	"testing"
)

type A struct{}

func TestRegister_NewActor(t *testing.T) {
	r := core.NewActorRegistry()
	r.Register("a", core.NewActor[A])
}
