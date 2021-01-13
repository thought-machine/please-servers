package please_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/thought-machine/please/src/core"
)

func TestBuildLabel(t *testing.T) {
	l := core.BuildLabel{PackageName: "src/core", Name: "core_test"}
	assert.Equal(t, "//src/core:core_test", l.String())
}
