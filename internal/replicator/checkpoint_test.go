package replicator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckDBsSame(t *testing.T) {
	want := dbPair{src: "urlshrinker", dst: "urlshrinker_v2"}

	err := checkDBs(want, want)

	assert.NoError(t, err)
}

func TestCheckDBs(t *testing.T) {
	want := dbPair{src: "urlshrinker", dst: "urlshrinker_v2"}
	for _, c := range []struct {
		name      string
		saved     dbPair
		wantSaved string
	}{
		{"другая база источника", dbPair{src: "a", dst: "urlshrinker_v2"}, `в чекпойнте "a" → "urlshrinker_v2"`},
		{"другая база цели", dbPair{src: "urlshrinker", dst: "urlshrinker"}, `в чекпойнте "urlshrinker" → "urlshrinker"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := checkDBs(c.saved, want)

			require.ErrorIs(t, err, ErrDBMismatch)
			assert.Contains(t, err.Error(), c.wantSaved)
			assert.Contains(t, err.Error(), `задано "urlshrinker" → "urlshrinker_v2"`)
		})
	}
}
