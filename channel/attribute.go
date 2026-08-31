package channel

import "sync"

type attributeKeyID string

// AttributeKey 是 Channel 级别的类型安全属性键。
type AttributeKey[T any] struct {
	name attributeKeyID
}

func NewAttributeKey[T any](name string) AttributeKey[T] {
	return AttributeKey[T]{name: attributeKeyID(name)}
}

func (k AttributeKey[T]) Name() string {
	return string(k.name)
}

func (k AttributeKey[T]) Get(attrs *AttributeMap) (T, bool) {
	var zero T
	if attrs == nil {
		return zero, false
	}
	value, ok := attrs.get(k.name)
	if !ok {
		return zero, false
	}
	typed, ok := value.(T)
	if !ok {
		return zero, false
	}
	return typed, true
}

func (k AttributeKey[T]) Set(attrs *AttributeMap, value T) {
	if attrs == nil {
		return
	}
	attrs.set(k.name, value)
}

func (k AttributeKey[T]) GetOrSet(attrs *AttributeMap, value T) T {
	if got, ok := k.Get(attrs); ok {
		return got
	}
	k.Set(attrs, value)
	return value
}

func (k AttributeKey[T]) Remove(attrs *AttributeMap) (T, bool) {
	var zero T
	if attrs == nil {
		return zero, false
	}
	value, ok := attrs.remove(k.name)
	if !ok {
		return zero, false
	}
	typed, ok := value.(T)
	if !ok {
		return zero, false
	}
	return typed, true
}

// AttributeAssignment 保存一个类型安全属性赋值，用于 Bootstrap 批量装配。
type AttributeAssignment struct {
	name  attributeKeyID
	value any
}

// Assignment 把属性键和值绑定成可复用的装配项。
func (k AttributeKey[T]) Assignment(value T) AttributeAssignment {
	return AttributeAssignment{name: k.name, value: value}
}

// AttributeMap 存储 Handler 间共享的 Channel 局部状态。
type AttributeMap struct {
	mu     sync.RWMutex
	values map[attributeKeyID]any
}

func NewAttributeMap() *AttributeMap {
	return &AttributeMap{}
}

// Clone 复制当前属性映射。属性值本身按接口引用复制，不深拷贝业务对象。
func (m *AttributeMap) Clone() *AttributeMap {
	out := NewAttributeMap()
	m.CopyTo(out)
	return out
}

// Apply 写入一组属性装配项，后出现的同名属性覆盖前值。
func (m *AttributeMap) Apply(assignments ...AttributeAssignment) {
	if m == nil {
		return
	}
	for _, assignment := range assignments {
		if assignment.name == "" {
			continue
		}
		m.set(assignment.name, assignment.value)
	}
}

// CopyTo 把当前属性复制到目标映射。该方法用于 Bootstrap 到 Channel 的配置快照传递。
func (m *AttributeMap) CopyTo(dst *AttributeMap) {
	if m == nil || dst == nil {
		return
	}
	values := m.snapshot()
	for key, value := range values {
		dst.set(key, value)
	}
}

func (m *AttributeMap) get(key attributeKeyID) (any, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, ok := m.values[key]
	return value, ok
}

func (m *AttributeMap) set(key attributeKeyID, value any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.values == nil {
		m.values = make(map[attributeKeyID]any, 4)
	}
	m.values[key] = value
}

func (m *AttributeMap) remove(key attributeKeyID) (any, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.values[key]
	if ok {
		delete(m.values, key)
	}
	return value, ok
}

func (m *AttributeMap) snapshot() map[attributeKeyID]any {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.values) == 0 {
		return nil
	}
	out := make(map[attributeKeyID]any, len(m.values))
	for key, value := range m.values {
		out[key] = value
	}
	return out
}
