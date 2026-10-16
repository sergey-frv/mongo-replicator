package replicator

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestIsHistoryLost(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"history lost 286", mongo.CommandError{Code: codeChangeStreamHistoryLost}, true},
		{"change stream fatal 280", mongo.CommandError{Code: codeChangeStreamFatal}, true},
		{"wrapped 286", fmt.Errorf("run: %w", mongo.CommandError{Code: codeChangeStreamHistoryLost}), true},
		{"double wrapped 280", fmt.Errorf("a: %w", fmt.Errorf("b: %w", mongo.CommandError{Code: codeChangeStreamFatal})), true},
		{"namespace not found 26", mongo.CommandError{Code: codeNamespaceNotFound}, false},
		{"wrapped other code", fmt.Errorf("x: %w", mongo.CommandError{Code: 11000}), false},
		{"plain error", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, IsHistoryLost(c.err))
		})
	}
}

func TestIsEventTooLarge(t *testing.T) {
	assert.True(t, IsEventTooLarge(mongo.CommandError{Code: codeBSONObjectTooLarge}))
	assert.True(t, IsEventTooLarge(fmt.Errorf("change stream: %w", mongo.CommandError{Code: codeBSONObjectTooLarge})))
	assert.False(t, IsEventTooLarge(mongo.CommandError{Code: codeChangeStreamHistoryLost}))
	assert.False(t, IsEventTooLarge(errors.New("boom")))
	assert.False(t, IsEventTooLarge(nil))
}

func TestIsDBCaseConflict(t *testing.T) {
	bulk := mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: codeDatabaseDifferCase}}}}
	assert.True(t, IsDBCaseConflict(fmt.Errorf("bulk write: %w", bulk)))
	assert.True(t, IsDBCaseConflict(mongo.CommandError{Code: codeDatabaseDifferCase}))
	assert.False(t, IsDBCaseConflict(mongo.CommandError{Code: codeNamespaceExists}))
	assert.False(t, IsDBCaseConflict(errors.New("boom")))
}

func TestHasCode(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		codes []int
		want  bool
	}{
		{"single match", mongo.CommandError{Code: 26}, []int{26}, true},
		{"one of many", mongo.CommandError{Code: 27}, []int{26, 27, 48}, true},
		{"wrapped match", fmt.Errorf("drop: %w", mongo.CommandError{Code: 48}), []int{26, 27, 48}, true},
		{"no match", mongo.CommandError{Code: 11000}, []int{26, 27, 48}, false},
		{"no codes", mongo.CommandError{Code: 26}, nil, false},
		{"write exception", mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000}}}, []int{11000}, true},
		{"plain error", errors.New("boom"), []int{26}, false},
		{"nil", nil, []int{26}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, hasCode(c.err, c.codes...))
		})
	}
}

func TestErrNoStartPoint(t *testing.T) {
	require.ErrorIs(t, fmt.Errorf("run: %w", ErrNoStartPoint), ErrNoStartPoint)
	assert.False(t, IsHistoryLost(ErrNoStartPoint))
}

func TestDropError(t *testing.T) {
	err := fmt.Errorf("run: %w", &DropError{NS: "db.c"})
	var de *DropError
	require.ErrorAs(t, err, &de)
	assert.False(t, de.Database)
	assert.Contains(t, err.Error(), "коллекция db.c")
	assert.Contains(t, (&DropError{NS: "db", Database: true}).Error(), "база db")
}
