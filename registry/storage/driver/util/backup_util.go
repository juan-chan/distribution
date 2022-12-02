package util

import (
	"fmt"
	"strings"
)

func GetBackupPath(path string) string {
	pathItem := strings.Split(path, "/")
	index := -1
	for i, item := range pathItem {
		if strings.EqualFold(item, "docker") {
			index = i
			break
		}
	}
	if index == -1 {
		return fmt.Sprintf("%s_%s", "backup", path)
	}
	result := append(pathItem[:index], fmt.Sprintf("%s_%s", pathItem[index], "backup"))
	result = append(result, pathItem[index+1:]...)
	return strings.Join(result, "/")
}
