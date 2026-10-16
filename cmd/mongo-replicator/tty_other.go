//go:build !unix

package main

import "os"

// foreground — проверка группы переднего плана есть только для Unix; без неё панель
// клавиатуру не читает.
func foreground(*os.File) bool { return false }
