//go:build integration

package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Пароль из терминала доходит до аутентификации, неверный — даёт isAuthFailed.
// mongod в compose без --auth, но учётные данные, переданные клиентом, проверяет.
func TestIntegrationPromptedPasswordAuth(t *testing.T) {
	srcURI := os.Getenv("IT_SRC_URI")
	require.NotEmpty(t, srcURI, "IT_SRC_URI не задан: интеграционные тесты запускаются через make test-integration")
	ctx := context.Background()

	admin, err := connect(ctx, mongoopts.Client().ApplyURI(srcURI))
	require.NoError(t, err)
	defer admin.Disconnect(ctx) //nolint:errcheck // тест
	const user, pwd = "it_prompt_user", "it-Pr0mpt-pass"
	db := admin.Database("admin")
	_ = db.RunCommand(ctx, bson.D{{Key: "dropUser", Value: user}}).Err() // от прошлого прогона
	require.NoError(t, db.RunCommand(ctx, bson.D{
		{Key: "createUser", Value: user}, {Key: "pwd", Value: pwd}, {Key: "roles", Value: bson.A{}},
	}).Err())
	defer db.RunCommand(ctx, bson.D{{Key: "dropUser", Value: user}}) //nolint:errcheck // тест

	uri := strings.Replace(srcURI, "://", "://"+user+"@", 1) + "&authSource=admin"
	try := func(answer string) error {
		p := &fakePrompter{terminal: true, answers: []string{answer}}
		opts, _, optsErr := clientOptions(srcSide(uri, false), p)
		require.NoError(t, optsErr)
		require.Len(t, p.prompts, 1)
		c, connErr := connect(ctx, opts)
		if connErr == nil {
			_ = c.Disconnect(ctx)
		}
		return connErr
	}

	require.NoError(t, try(pwd))
	err = try("wrong-pass")
	require.Error(t, err)
	assert.True(t, isAuthFailed(err), "ошибка: %v", err)
	assert.NotContains(t, err.Error(), "wrong-pass")
}
