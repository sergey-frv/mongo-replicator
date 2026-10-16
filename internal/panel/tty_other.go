//go:build !unix

package panel

import "os"

// foreground — проверка группы переднего плана есть только для Unix; без неё панель
// клавиатуру не читает.
func foreground(*os.File) bool { return false }
