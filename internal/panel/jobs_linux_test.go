package panel

import (
	"os"
	"os/signal"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestSigIgnored(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status string
		want   bool
	}{
		{name: "SIGTSTP в маске", status: "SigPnd:\t0000000000000000\nSigIgn:\t0000000000080000\nSigCgt:\t0\n", want: true},
		{name: "маска без SIGTSTP", status: "SigIgn:\t0000000000001000\nSigCgt:\t0\n", want: false},
		{name: "нет маски", status: "Name:\tmongo-replicator\n", want: true},
		{name: "битая маска", status: "SigIgn:\tzz\n", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, sigIgnored(tc.status, unix.SIGTSTP))
		})
	}
}

// SIG_IGN, установленный процессом (как унаследованный от родителя), виден в маске
// SigIgn его /proc/self/status.
func TestSigIgnoredSeesIgnoredTSTP(t *testing.T) {
	signal.Ignore(unix.SIGTSTP)
	t.Cleanup(func() { signal.Reset(unix.SIGTSTP) })

	status, err := os.ReadFile("/proc/self/status")
	require.NoError(t, err)

	assert.True(t, sigIgnored(string(status), unix.SIGTSTP))
}
