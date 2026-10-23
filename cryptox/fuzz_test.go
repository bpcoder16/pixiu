package cryptox

import (
	"bytes"
	"encoding/base64"
	"testing"
)

func FuzzDecrypt(f *testing.F) {
	c, _ := NewAESGCM(make([]byte, 32))
	valid, _ := c.Encrypt([]byte("resource-42"), []byte("share"))
	f.Add(valid, []byte("share"))
	f.Add([]byte{}, []byte{})
	f.Add(make([]byte, 28), []byte{})
	f.Fuzz(func(t *testing.T, encrypted, aad []byte) {
		before := bytes.Clone(encrypted)
		plain, err := c.Decrypt(encrypted, aad)
		if err != nil && plain != nil {
			t.Fatal("失败返回明文")
		}
		if !bytes.Equal(encrypted, before) {
			t.Fatal("修改了密文")
		}
	})
}

func FuzzDecryptToken(f *testing.F) {
	c, _ := NewAESGCM(make([]byte, 32))
	valid, _ := c.EncryptToken([]byte("resource-42"), "share")
	f.Add(valid, "share")
	f.Add("v1.AAAA\n", "share")
	f.Add("", "")
	f.Fuzz(func(t *testing.T, token, purpose string) {
		plain, err := c.DecryptToken(token, purpose)
		if err != nil {
			if plain != nil {
				t.Fatal("失败返回明文")
			}
			return
		}
		raw, err := base64.RawURLEncoding.Strict().DecodeString(token[3:])
		if err != nil || "v1."+base64.RawURLEncoding.EncodeToString(raw) != token {
			t.Fatal("接受了非规范编码")
		}
		if got, err := c.DecryptToken(token, purpose+"!"); err == nil || got != nil {
			t.Fatal("用途绑定失败")
		}
	})
}
