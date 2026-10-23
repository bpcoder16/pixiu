package cryptox_test

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/bpcoder16/pixiu/cryptox"
)

func ExampleHMACSHA256() {
	// 生产密钥由安全配置加载；此处临时生成仅为完整示例。
	key, err := cryptox.RandomBytes(32)
	if err != nil {
		panic(err)
	}
	signer, err := cryptox.NewHMACSHA256(key)
	if err != nil {
		panic(err)
	}
	nonce, err := cryptox.RandomToken(16)
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequest(http.MethodPost, "https://internal.example/jobs?queue=normal", nil)
	if err != nil {
		panic(err)
	}
	body := []byte(`{"job":"42"}`)
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	message, err := requestMessage(req, body, timestamp, nonce)
	if err != nil {
		panic(err)
	}
	signature, err := signer.Sign(message)
	if err != nil {
		panic(err)
	}
	// 接收端从请求重建相同 message，再验证签名；成功后还须校验时间窗口并原子登记 nonce。
	fmt.Println(signer.Verify(message, signature))
	// Output: true
}

// requestMessage 是应用协议示例，不是 cryptox 的 HTTP 公共 API。
// 签名放在独立 header 中；body 为读取了大小受限的请求体后取得的原始字节。
func requestMessage(req *http.Request, body []byte, timestamp, nonce string) ([]byte, error) {
	query, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, err
	}
	for _, values := range query {
		if len(values) != 1 {
			return nil, fmt.Errorf("duplicate query parameter")
		}
	}
	digest := sha256.Sum256(body)
	fields := []string{
		"cryptox:request:v1",
		req.Method,
		req.Host,
		req.URL.EscapedPath(),
		query.Encode(),
		string(digest[:]),
		timestamp,
		nonce,
	}
	var message []byte
	for _, field := range fields {
		message = binary.AppendUvarint(message, uint64(len(field)))
		message = append(message, field...)
	}
	return message, nil
}

func ExampleAESGCM_EncryptToken() {
	key, err := cryptox.RandomBytes(32)
	if err != nil {
		panic(err)
	}
	box, err := cryptox.NewAESGCM(key)
	if err != nil {
		panic(err)
	}
	type share struct {
		Resource string `json:"resource"`
		Scope    string `json:"scope"`
		Expires  int64  `json:"exp"`
	}
	now := time.Now()
	payload, err := json.Marshal(share{
		Resource: "file-42",
		Scope:    "download",
		Expires:  now.Add(5 * time.Minute).Unix(),
	})
	if err != nil {
		panic(err)
	}
	token, err := box.EncryptToken(payload, "share-download")
	if err != nil {
		panic(err)
	}
	decrypted, err := box.DecryptToken(token, "share-download")
	if err != nil {
		panic(err)
	}
	var claims share
	if err := json.Unmarshal(decrypted, &claims); err != nil {
		panic(err)
	}
	if time.Now().Unix() >= claims.Expires || claims.Resource == "" || claims.Scope != "download" {
		panic("invalid share claims")
	}
	// 应用继续检查资源是否仍可分享及撤销状态，再执行下载。
	fmt.Println(claims.Resource, claims.Scope)
	// Output: file-42 download
}

func ExampleRandomToken() {
	token, err := cryptox.RandomToken(32)
	if err != nil {
		panic(err)
	}
	// 服务端保存 token 的摘要及资源、有效期；一次性使用须原子消费该记录。
	fmt.Println(len(token))
	// Output: 43
}
