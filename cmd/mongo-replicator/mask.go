package main

import (
	"net/url"
	"strings"
)

// maskURI заменяет пароль в URI на xxxxx. Разбор строковый, а не url.Parse:
// multi-host URI (h1:27017,h2:27017) net/url может не разобрать. Userinfo
// ищем до первого "/": драйвер допускает в пароле "?", но не "/".
func maskURI(uri string) string {
	start := schemeEnd(uri)
	if start < 0 {
		return uri
	}
	rest := uri[start:]
	end := strings.IndexByte(rest, '/')
	if end < 0 {
		end = len(rest)
	}
	at := strings.LastIndex(rest[:end], "@")
	if at < 0 {
		return uri
	}
	colon := strings.Index(rest[:at], ":")
	if colon < 0 {
		return uri
	}
	return uri[:start+colon+1] + "xxxxx" + uri[start+at:]
}

// schemeEnd — индекс после "<схема>://" в начале URI, -1 — схемы нет. "://" дальше
// в строке (mongodb:u:pw@h://x) схемой не считается: иначе пароль до него не ищется.
func schemeEnd(uri string) int {
	i := strings.Index(uri, "://")
	if i <= 0 {
		return -1
	}
	for _, r := range uri[:i] {
		switch {
		case 'a' <= r && r <= 'z', 'A' <= r && r <= 'Z', '0' <= r && r <= '9', strings.ContainsRune("+-.", r):
		default:
			return -1
		}
	}
	return i + len("://")
}

// secretOptions — параметры URI (ключи в нижнем регистре), значения которых секретны.
var secretOptions = map[string]bool{
	"tlscertificatekeyfilepassword":   true,
	"sslclientcertificatekeypassword": true,
	"authmechanismproperties":         true, // AWS_SESSION_TOKEN:...
}

// maskOptions заменяет на xxxxx значения секретных параметров URI. Драйвер берёт query
// после первого "?" и делит пары только по "&" и ";": "?" в значении — часть значения.
// Первый "?" query строкой не найти ("?" допустим и в имени пользователя), поэтому
// делим по "&" и ";", а ключ ищем в начале пары и после каждого "?" в ней (maskPair).
func maskOptions(uri string) string {
	var b strings.Builder
	for {
		i := strings.IndexAny(uri, "&;")
		if i < 0 {
			b.WriteString(maskPair(uri))
			return b.String()
		}
		b.WriteString(maskPair(uri[:i]))
		b.WriteByte(uri[i])
		uri = uri[i+1:]
	}
}

// maskPair маскирует до конца пары значение первого секретного ключа, найденного
// в её начале или после "?" (tlsCertificateKeyFilePassword=a?b — значение "a?b").
func maskPair(pair string) string {
	for start := 0; ; {
		if key, _, ok := strings.Cut(pair[start:], "="); ok && secretKey(key) {
			return pair[:start] + key + "=xxxxx"
		}
		q := strings.IndexByte(pair[start:], '?')
		if q < 0 {
			return pair
		}
		start += q + 1
	}
}

// secretKey — ключ секретного параметра. Сравниваем после unescape: драйвер принимает
// и экранированный ключ. Неразбираемый ключ считаем секретным.
func secretKey(key string) bool {
	k, err := url.QueryUnescape(key)
	return err != nil || secretOptions[strings.ToLower(k)]
}

// shownURI — URI для вывода без разбора драйвером: с замаскированными паролем и секретными параметрами.
// false — выводить нельзя: пароль некорректного URI maskURI не нашёл (неэкранированный "/")
// или нашёл не целиком (неэкранированные "@" и "/": mongodb://u:p@x/Secret@h/ — хвост "x/Secret").
// Лишний "@" после схемы корректному URI с паролем не нужен, ложное срабатывание лишь прячет URI.
func shownURI(uri string) (string, bool) {
	userMasked := maskURI(uri)
	masked := maskOptions(userMasked)
	if userMasked == uri {
		return masked, !mayHavePassword(uri)
	}
	return masked, strings.Count(userMasked[schemeEnd(userMasked):], "@") == 1
}

// logURI — URI для лога до его разбора: с замаскированными секретами, а если пароль
// может остаться в тексте (некорректный URI) — не выводит URI вовсе.
func logURI(uri string) string {
	if shown, ok := shownURI(uri); ok {
		return shown
	}
	return "(скрыт: в нём может быть пароль)"
}
