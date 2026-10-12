package ginauth_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/bpcoder16/pixiu/biz/httpauth"
	"github.com/bpcoder16/pixiu/biz/httpauth/ginauth"
	"github.com/gin-gonic/gin"
)

func TestRoutesSelectIndependentSchemes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	registry := httpauth.NewRegistry()
	var appCalls, adminCalls int
	registry.MustRegister("app", httpauth.AuthenticatorFunc(func(r *http.Request) (*httpauth.Principal, error) {
		appCalls++
		token, err := httpauth.BearerToken(r)
		if err != nil || token != "app-token" {
			return nil, httpauth.ErrUnauthenticated
		}
		return &httpauth.Principal{UserID: "42"}, nil
	}))
	registry.MustRegister("admin", httpauth.AuthenticatorFunc(func(r *http.Request) (*httpauth.Principal, error) {
		adminCalls++
		cookie, err := r.Cookie("session")
		if err != nil || cookie.Value != "admin-session" {
			return nil, httpauth.ErrUnauthenticated
		}
		return &httpauth.Principal{UserID: "42"}, nil
	}))
	guard := ginauth.New(registry)
	router := gin.New()
	handler := func(c *gin.Context) {
		principal, ok := httpauth.PrincipalFromContext(c.Request.Context())
		scheme, schemeOK := httpauth.SchemeFromContext(c.Request.Context())
		if !ok || !schemeOK {
			t.Fatal("标准 Context 缺少认证结果")
		}
		c.String(http.StatusOK, scheme+":"+principal.UserID)
	}
	router.GET("/app", guard.Require("app"), handler)
	router.GET("/admin", guard.Require("admin"), handler)
	for _, tc := range []struct {
		path   string
		bearer string
		cookie string
		status int
		body   string
	}{
		{"/app", "app-token", "", 200, "app:42"},
		{"/admin", "", "admin-session", 200, "admin:42"},
		{"/admin", "app-token", "", 401, ""},
		{"/app", "", "admin-session", 401, ""},
	} {
		r := httptest.NewRequest(http.MethodGet, tc.path, nil)
		if tc.bearer != "" {
			r.Header.Set("Authorization", "Bearer "+tc.bearer)
		}
		if tc.cookie != "" {
			r.AddCookie(&http.Cookie{Name: "session", Value: tc.cookie})
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != tc.status || w.Body.String() != tc.body {
			t.Fatalf("%s: %d %q", tc.path, w.Code, w.Body.String())
		}
	}
	if appCalls != 2 || adminCalls != 2 {
		t.Fatalf("不应跨方案回退: app=%d admin=%d", appCalls, adminCalls)
	}
}

func TestFailuresAbortAndPreserveErrors(t *testing.T) {
	dependencyErr := errors.New("database credentials must stay private")
	for _, tc := range []struct {
		name      string
		principal *httpauth.Principal
		err       error
		wantErr   error
		status    int
	}{
		{"unauthenticated", nil, fmt.Errorf("expired: %w", httpauth.ErrUnauthenticated), httpauth.ErrUnauthenticated, 401},
		{"dependency", nil, dependencyErr, dependencyErr, 500},
		{"identity with error", &httpauth.Principal{UserID: "42"}, dependencyErr, dependencyErr, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, custom := range []bool{false, true} {
				registry := httpauth.NewRegistry()
				registry.MustRegister("app", httpauth.AuthenticatorFunc(func(*http.Request) (*httpauth.Principal, error) {
					return tc.principal, tc.err
				}))
				var options []ginauth.Option
				var failureCalled bool
				if custom {
					options = append(options, ginauth.WithFailureHandler(func(c *gin.Context, err error) {
						failureCalled = true
						if !c.IsAborted() || !errors.Is(err, tc.wantErr) {
							t.Fatalf("失败契约错误: aborted=%v err=%v", c.IsAborted(), err)
						}
						// 回调故意不调用 Abort，由公共中间件保证终止。
						c.Status(http.StatusOK)
					}))
				}
				router := gin.New()
				router.GET("/", ginauth.New(registry, options...).Require("app"), func(*gin.Context) {
					t.Fatal("认证失败不应到达 handler")
				})
				w := httptest.NewRecorder()
				router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
				want := tc.status
				if custom {
					want = http.StatusOK
				}
				if w.Code != want || w.Body.Len() != 0 || failureCalled != custom {
					t.Fatalf("失败响应错误: status=%d body=%q callback=%v", w.Code, w.Body.String(), failureCalled)
				}
			}
		})
	}
}

func TestSuccessfulAuthenticationPreservesIdentity(t *testing.T) {
	for _, userID := range []string{"42", "", " \t"} {
		t.Run(fmt.Sprintf("UserID=%q", userID), func(t *testing.T) {
			registry := httpauth.NewRegistry()
			registry.MustRegister("platform", httpauth.AuthenticatorFunc(func(*http.Request) (*httpauth.Principal, error) {
				return &httpauth.Principal{UserID: userID}, nil
			}))
			guard := ginauth.New(registry, ginauth.WithFailureHandler(func(*gin.Context, error) {
				t.Fatal("认证器返回成功时不应调用失败回调")
			}))
			router := gin.New()
			called := false
			router.GET("/", guard.Require("platform"), func(c *gin.Context) {
				called = true
				principal, ok := httpauth.PrincipalFromContext(c.Request.Context())
				if !ok || principal.UserID != userID {
					t.Fatalf("身份应原样传递: principal=%v ok=%v", principal, ok)
				}
				c.Status(http.StatusNoContent)
			})
			w := httptest.NewRecorder()
			router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
			if !called || w.Code != http.StatusNoContent {
				t.Fatalf("认证成功应放行: called=%v status=%d", called, w.Code)
			}
		})
	}
}

func TestCancellationHandlingUsesAuthenticatorResult(t *testing.T) {
	for _, tc := range []struct {
		name         string
		cancelBefore bool
		returnError  bool
		status       int
	}{
		{"canceled before success", true, false, http.StatusOK},
		{"canceled during success", false, false, http.StatusOK},
		{"canceled before error", true, true, http.StatusServiceUnavailable},
		{"canceled during error", false, true, http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			registry := httpauth.NewRegistry()
			called := false
			registry.MustRegister("app", httpauth.AuthenticatorFunc(func(r *http.Request) (*httpauth.Principal, error) {
				called = true
				if r.Context() != ctx {
					t.Fatal("认证器应收到原请求 Context")
				}
				cancel()
				if tc.returnError {
					return nil, fmt.Errorf("credential lookup: %w", r.Context().Err())
				}
				return &httpauth.Principal{UserID: "42"}, nil
			}))
			if tc.cancelBefore {
				cancel()
			}
			failureCalled := false
			guard := ginauth.New(registry, ginauth.WithFailureHandler(func(c *gin.Context, err error) {
				failureCalled = true
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("应保留取消错误: %v", err)
				}
				c.Status(http.StatusServiceUnavailable)
			}))
			router := gin.New()
			handlerCalled := false
			router.GET("/", guard.Require("app"), func(c *gin.Context) {
				handlerCalled = true
				principal, ok := httpauth.PrincipalFromContext(c.Request.Context())
				if !ok || principal.UserID != "42" || !errors.Is(c.Request.Context().Err(), context.Canceled) {
					t.Fatal("下游应收到已验证身份并保留 Context 取消状态")
				}
				c.Status(http.StatusOK)
			})
			w := httptest.NewRecorder()
			router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx))
			if w.Code != tc.status || failureCalled != tc.returnError || !called || handlerCalled == tc.returnError {
				t.Fatalf("取消处理错误: status=%d failure=%v called=%v handler=%v", w.Code, failureCalled, called, handlerCalled)
			}
		})
	}
}

func TestConcurrentRequestsKeepSeparateIdentities(t *testing.T) {
	registry := httpauth.NewRegistry()
	registry.MustRegister("app", httpauth.AuthenticatorFunc(func(r *http.Request) (*httpauth.Principal, error) {
		return &httpauth.Principal{UserID: r.Header.Get("X-Test-User")}, nil
	}))
	router := gin.New()
	router.GET("/", ginauth.New(registry).Require("app"), func(c *gin.Context) {
		p, _ := httpauth.PrincipalFromContext(c.Request.Context())
		c.String(http.StatusOK, p.UserID)
	})
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			id := fmt.Sprint(i)
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Header.Set("X-Test-User", id)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			if w.Code != 200 || w.Body.String() != id {
				t.Errorf("请求串身份: want=%s got=%d %q", id, w.Code, w.Body.String())
			}
		})
	}
	wg.Wait()
}

func TestInvalidConfigurationPanicsBeforeServing(t *testing.T) {
	for _, run := range []func(){
		func() { ginauth.New(nil) },
		func() { ginauth.New(httpauth.NewRegistry(), nil) },
		func() { ginauth.New(httpauth.NewRegistry(), ginauth.WithFailureHandler(nil)) },
		func() { ginauth.New(httpauth.NewRegistry()).Require("missing") },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("无效配置应在启动阶段 panic")
				}
			}()
			run()
		}()
	}
}
