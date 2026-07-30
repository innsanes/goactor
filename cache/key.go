package cache

import (
	"fmt"
	"strings"
)

func BuildKey(hashKey string, prefix string, parts ...string) string {
	str := make([]string, 0, len(parts)+2)
	str = append(str, fmt.Sprintf("{%s}", hashKey))
	str = append(str, prefix)
	str = append(str, parts...)
	return strings.Join(str, ":")
}
