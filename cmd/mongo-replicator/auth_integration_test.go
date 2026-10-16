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
	ctx := context.Background()
	srcURI := os.Getenv("IT_SRC_URI")
	require.NotEmpty(t, srcURI, "IT_SRC_URI не задан: интеграционные тесты запускаются через make test-integration")
	const user, pwd = "it_prompt_user", "it-Pr0mpt-pass"
	createUser(ctx, t, srcURI, user, pwd)
	uri := strings.Replace(srcURI, "://", "://"+user+"@", 1) + "&authSource=admin"

	okErr := promptAndConnect(ctx, t, uri, pwd)
	wrongErr := promptAndConnect(ctx, t, uri, "wrong-pass")

	require.NoError(t, okErr, "верный пароль из терминала")
	require.Error(t, wrongErr, "неверный пароль из терминала")
	assert.True(t, isAuthFailed(wrongErr), "ошибка аутентификации: %v", wrongErr)
	assert.NotContains(t, wrongErr.Error(), "wrong-pass", "пароля нет в тексте ошибки")
}

// createUser создаёт пользователя без ролей в admin; после теста он удаляется.
func createUser(ctx context.Context, t *testing.T, uri, user, pwd string) {
	t.Helper()
	admin, err := connect(ctx, mongoopts.Client().ApplyURI(uri))
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Disconnect(ctx) })
	db := admin.Database("admin")
	_ = db.RunCommand(ctx, bson.D{{Key: "dropUser", Value: user}}).Err() // от прошлого прогона
	require.NoError(t, db.RunCommand(ctx, bson.D{
		{Key: "createUser", Value: user}, {Key: "pwd", Value: pwd}, {Key: "roles", Value: bson.A{}},
	}).Err())
	t.Cleanup(func() { _ = db.RunCommand(ctx, bson.D{{Key: "dropUser", Value: user}}).Err() })
}

// promptAndConnect получает опции с паролем answer из терминала и подключается; возвращает ошибку подключения.
func promptAndConnect(ctx context.Context, t *testing.T, uri, answer string) error {
	t.Helper()
	p := &fakePrompter{terminal: true, answers: []string{answer}}
	opts, _, err := clientOptions(srcSide(uri, false), p)
	require.NoError(t, err)
	require.Len(t, p.prompts, 1, "один запрос пароля")
	c, err := connect(ctx, opts)
	if err == nil {
		_ = c.Disconnect(ctx)
	}
	return err
}
