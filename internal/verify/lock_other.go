//go:build !unix || aix

package verify

// lock — flock есть только на Unix (кроме AIX); без него второй одновременный
// запуск сверки той же базы не отвергается.
func lock(string) (unlock func(), err error) { return func() {}, nil }
