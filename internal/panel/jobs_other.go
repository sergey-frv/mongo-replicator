//go:build !linux

package panel

// jobControl — проверка управления заданиями есть только для Linux; без неё Ctrl+Z
// игнорируется.
func jobControl() bool { return false }
