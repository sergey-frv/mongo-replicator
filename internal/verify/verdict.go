// Package verify — сверка документов, изменённых после заданного момента, и схемы
// базы между источником и целью репликации при живых записях.
package verify

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// maxDiffFields — сколько имён полей diffFields отдаёт для лога.
const maxDiffFields = 10

// result — вердикт по одному ключу.
type result int

const (
	matched result = iota // цель совпала с источником
	defect                // источник не менялся, цель расходится
	hot                   // источник менялся во время проверки — повторить
)

// verdict сравнивает документ цели D с двумя чтениями источника: S1 (до
// ожидания чекпойнта, время T1) и S2 (после чтения D, время T2). nil — документа
// нет; это обычное значение, так ловятся лишние документы и недошедшие удаления.
// eventInWindow — у ключа есть событие в (T1, T2]: тогда S1 == S2 не значит,
// что источник не менялся (ABA), и расхождение — не дефект.
//
// Сравнение побайтное: репликатор пишет fullDocument как replace, порядок полей
// сохраняется.
func verdict(s1, d, s2 bson.Raw, eventInWindow bool) result {
	switch {
	case bytes.Equal(d, s1), bytes.Equal(d, s2):
		return matched
	case bytes.Equal(s1, s2) && !eventInWindow:
		return defect
	default:
		return hot
	}
}

// sampled — берётся ли ключ в выборку доли p: первые 8 байт sha256 от типа и
// значения _id как uint64 меньше p·2⁶⁴. Решение зависит только от _id, поэтому
// после перезапуска выбираются те же ключи.
func sampled(id bson.RawValue, p float64) bool {
	if p >= 1 {
		return true
	}
	sum := sha256.Sum256(append([]byte{byte(id.Type)}, id.Value...))
	return float64(binary.BigEndian.Uint64(sum[:8])) < p*0x1p64
}

// diffFields — имена верхнеуровневых полей, которые различаются или есть только
// в одном из документов, не больше maxDiffFields: сначала поля src в его
// порядке, затем лишние поля dst в его порядке. Документы, различающиеся только
// порядком полей, дают пустой список.
func diffFields(src, dst bson.Raw) []string {
	// документы прочитаны с сервера и разбираются; при ошибке Elements
	// возвращает nil, и в дифф попадут поля другой стороны
	srcEls, _ := src.Elements()
	dstEls, _ := dst.Elements()
	var diff []string
	for _, el := range srcEls {
		v, err := dst.LookupErr(el.Key())
		if err != nil || !v.Equal(el.Value()) {
			diff = append(diff, el.Key())
		}
	}
	for _, el := range dstEls {
		if _, err := src.LookupErr(el.Key()); err != nil {
			diff = append(diff, el.Key())
		}
	}
	if len(diff) > maxDiffFields {
		diff = diff[:maxDiffFields]
	}
	return diff
}
