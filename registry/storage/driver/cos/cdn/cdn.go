package cdn

import (
	"crypto/md5"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"time"
)

const (
	SignName = "sign"

	// 文件路径-timestamp-rand-uid-自定义密钥
	MD5Source = "%s-%d-%s-0-%s"

	// timestamp-rand-uid-md5hash
	Sign = "%d-%s-0-%s"

	// DomainName/Filename?sign=timestamp-rand-uid-md5hash
	TypeA = "%s%s?" + SignName + "=%s"
)

type CDNConfig struct {
	Domain string
	Key    string
}

func PreSignedCDNURLTypeA(config CDNConfig, name string) (*url.URL, error) {
	if name == "" {
		return nil, nil
	}

	index := strings.Index(name, "/")
	if index != 0 {
		name = "/" + name
	}

	timestamp := time.Now().Unix()
	rand := RandStringRunes(8)

	md5Src := fmt.Sprintf(MD5Source, name, timestamp, rand, config.Key)
	md5Bytes := md5.Sum([]byte(md5Src))
	md5Hash := fmt.Sprintf("%x", md5Bytes)

	sign := fmt.Sprintf(Sign, timestamp, rand, md5Hash)

	rawURL := fmt.Sprintf(TypeA, config.Domain, name, sign)
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	return parsedURL, nil
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
