package structs

type BoolValue[T any] struct {
	enabled bool
	value   T
}

func NewBoolValue[T any]() *BoolValue[T] {
	return &BoolValue[T]{
		enabled: false,
	}
}

func (v *BoolValue[T]) IsEnabled() bool {
	return v.enabled
}

func (v *BoolValue[T]) Get() (T, bool) {
	return v.value, v.enabled
}

func (v *BoolValue[T]) Enable(value T) {
	if v.enabled {
		return
	}
	v.enabled = true
	v.value = value
}

func (v *BoolValue[T]) UpdateEnable(value T) {
	v.enabled = true
	v.value = value
}

func (v *BoolValue[T]) Disable() {
	v.enabled = false
}
