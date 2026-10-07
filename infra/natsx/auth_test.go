package natsx

import (
	"errors"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// 使用真实服务端握手，验证认证字段生效，而非仅检查客户端选项。
func TestNewConfigAuthentication(t *testing.T) {
	for _, auth := range []struct {
		name     string
		username string
		password string
		token    string
	}{
		{
			name:     "user_password",
			username: " auth-user ",
			password: " auth-password:@/%?# ",
		},
		{
			name:     "empty_password",
			username: "auth-empty-password-user",
		},
		{
			name:  "token_passthrough",
			token: " auth-token:@/%?# ",
		},
	} {
		t.Run(auth.name, func(t *testing.T) {
			s, err := server.NewServer(&server.Options{
				Host:          "127.0.0.1",
				Port:          -1,
				NoLog:         true,
				NoSigs:        true,
				Username:      auth.username,
				Password:      auth.password,
				Authorization: auth.token,
			})
			if err != nil {
				t.Fatal(err)
			}
			s.Start()
			t.Cleanup(func() {
				s.Shutdown()
				s.WaitForShutdown()
			})
			if !s.ReadyForConnections(5 * time.Second) {
				t.Fatal("认证服务端未就绪")
			}
			methods := []string{"url", "wrong", "missing"}
			if auth.token == "" {
				methods = append(methods, "config", "callback", "wrong_callback")
			} else {
				methods = append(methods, "option")
			}
			for _, method := range methods {
				t.Run(method, func(t *testing.T) {
					buf := captureLogs(t)
					cfg := Config{
						Name:           "auth-test",
						URLs:           []string{s.ClientURL()},
						ConnectTimeout: time.Second,
					}
					var options []nats.Option
					switch method {
					case "config", "wrong":
						if auth.token != "" {
							options = append(options, nats.Token("wrong-auth-token"))
							break
						}
						cfg.Username = auth.username
						cfg.Password = auth.password
						if method == "wrong" {
							cfg.Password = "wrong-auth-password"
						}
					case "option":
						options = append(options, nats.Token(auth.token))
					case "callback", "wrong_callback":
						options = append(options, nats.UserInfoHandler(func() (string, string) {
							if method == "wrong_callback" {
								return auth.username, "wrong-auth-password"
							}
							return auth.username, auth.password
						}))
					case "url":
						u, err := url.Parse(s.ClientURL())
						if err != nil {
							t.Fatal(err)
						}
						if auth.token != "" {
							u.User = url.User(auth.token)
						} else {
							u.User = url.UserPassword(auth.username, auth.password)
						}
						cfg.URLs = []string{u.String()}
					}
					c, err := New(cfg, options...)
					if method == "wrong" || method == "wrong_callback" || method == "missing" {
						if c != nil {
							_ = c.Close()
							t.Fatal("无效凭据意外建立连接")
						}
						if !errors.Is(err, ErrConnect) || !errors.Is(err, nats.ErrAuthorization) {
							t.Fatalf("认证失败未保留错误类别: %v", err)
						}
						var found bool
						for _, record := range readLogs(t, buf) {
							errorText, ok := record["err"].(string)
							if record["nats_event"] == "connect_failed" && ok && errorText != "" && strings.Contains(err.Error(), errorText) {
								found = true
							}
						}
						if !found {
							t.Fatal("缺少认证失败原始错误日志")
						}
					} else {
						if err != nil {
							t.Fatalf("合法凭据连接失败: %v", err)
						}
						if err := c.Close(); err != nil {
							t.Fatalf("认证连接关闭失败: %v", err)
						}
					}
				})
			}
		})
	}
}

type authCountingDialer struct{ calls int }

func (d *authCountingDialer) Dial(string, string) (net.Conn, error) {
	d.calls++
	return nil, errors.New("unexpected auth dial")
}

func TestNewAuthConflictsBeforeDial(t *testing.T) {
	for _, tc := range []struct {
		name         string
		cfg          Config
		option       nats.Option
		noConfigAuth bool
	}{
		{
			name: "password_without_username",
			cfg: Config{
				Password: "private-password",
			},
		},
		{
			name:   "user_info_option",
			option: nats.UserInfo("private-user", "private-password"),
		},
		{
			name:         "user_info_without_config",
			option:       nats.UserInfo("private-user", "private-password"),
			noConfigAuth: true,
		},
		{
			name:         "username_option_without_config",
			option:       nats.UserInfo("private-user", ""),
			noConfigAuth: true,
		},
		{
			name:         "password_option_without_config",
			option:       nats.UserInfo("", "private-password"),
			noConfigAuth: true,
		},
		{
			name:   "token_option",
			option: nats.Token("private-token"),
		},
		{
			name:   "user_info_callback",
			option: nats.UserInfoHandler(func() (string, string) { return "private-user", "private-password" }),
		},
		{
			name: "user_info_callback_with_empty_password",
			cfg: Config{
				Username: "private-user",
			},
			option: nats.UserInfoHandler(func() (string, string) { return "private-user", "private-password" }),
		},
		{
			name:   "token_callback",
			option: nats.TokenHandler(func() string { return "private-token" }),
		},
		{
			name:   "nkey_option",
			option: nats.Nkey("private-nkey", func([]byte) ([]byte, error) { return nil, nil }),
		},
		{
			name: "jwt_option",
			option: nats.UserJWT(
				func() (string, error) { return "private-jwt", nil },
				func([]byte) ([]byte, error) { return nil, nil },
			),
		},
		{
			name: "url_user_password",
			cfg:  Config{URLs: []string{"nats://private-user:private-password@127.0.0.1:4222"}},
		},
		{
			name: "url_without_scheme",
			cfg:  Config{URLs: []string{"private-user:private-password@127.0.0.1:4222"}},
		},
		{
			name: "second_url_token",
			cfg:  Config{URLs: []string{nats.DefaultURL, "nats://private-token@127.0.0.1:4223"}},
		},
		{
			name: "malformed_url",
			cfg:  Config{URLs: []string{"nats://private-user:private-%zz@127.0.0.1:4222"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureLogs(t)
			cfg := tc.cfg
			cfg.Name = "auth-conflict"
			if len(cfg.URLs) == 0 {
				cfg.URLs = []string{nats.DefaultURL}
			}
			if !tc.noConfigAuth && cfg.Username == "" && cfg.Password == "" {
				cfg.Username = "private-user"
				cfg.Password = "private-password"
			}
			dialer := &authCountingDialer{}
			c, err := New(cfg, tc.option, nats.SetCustomDialer(dialer))
			if c != nil {
				_ = c.Close()
				t.Fatal("冲突的认证配置意外建立连接")
			}
			if err == nil || errors.Is(err, ErrConnect) || dialer.calls != 0 {
				t.Fatalf("未在连接前拒绝认证配置: err=%v, dials=%d", err, dialer.calls)
			}
			if tc.name == "malformed_url" {
				parseErr, ok := errors.AsType[*url.Error](err)
				if !ok || parseErr.URL != cfg.URLs[0] || !strings.Contains(err.Error(), parseErr.Error()) {
					t.Fatalf("未保留原始 URL 解析错误: %v", err)
				}
			} else if strings.Contains(err.Error()+buf.String(), "private-") {
				t.Fatal("配置错误或日志泄露凭据")
			}
		})
	}
}
