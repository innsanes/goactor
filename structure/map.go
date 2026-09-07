package structure

func MapIncrease[K comparable, V int | int8 | int16 | int32 | int64](m map[K]V, key K, add V) (result V) {
	if m == nil {
		m = make(map[K]V)
	}
	_, ok := m[key]
	if !ok {
		m[key] = add
	} else {
		m[key] += add
	}
	return m[key]
}

func MapExist[K comparable, V any](m map[K]V, key K) bool {
	_, ok := m[key]
	return ok
}

func MapAddIfNotExist[K comparable, V any](m map[K]V, key K, value V) {
	_, ok := m[key]
	if !ok {
		m[key] = value
	}
	return
}

func MapAdd[K comparable, V any](m map[K]V, key K, value V) {
	m[key] = value
	return
}
