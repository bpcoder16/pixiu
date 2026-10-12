package httpauth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRegistryRejectsInvalidRegistrationAndFreezesOnResolve(t *testing.T) {
	valid := AuthenticatorFunc(func(*http.Request) (*Principal, error) {
		return &Principal{UserID: "42"}, nil
	})
	var nilPointer *testAuthenticator
	var nilFunc AuthenticatorFunc
	for _, tc := range []struct {
		name string
		auth Authenticator
	}{
		{"", valid},
		{" app", valid},
		{"app ", valid},
		{"app", nil},
		{"app", nilPointer},
		{"app", nilFunc},
	} {
		if err := NewRegistry().Register(tc.name, tc.auth); err == nil {
			t.Fatalf("应拒绝无效注册: name=%q, auth=%T", tc.name, tc.auth)
		}
	}
	var registry Registry
	registry.MustRegister("app", valid)
	if err := registry.Register("app", valid); err == nil {
		t.Fatal("重复注册应报错")
	}
	resolved, err := registry.Resolve("app")
	if err != nil {
		t.Fatal(err)
	}
	principal, err := resolved.Authenticate(httptest.NewRequest(http.MethodGet, "/", nil))
	if err != nil || principal.UserID != "42" {
		t.Fatalf("解析错误: %v, %v", principal, err)
	}
	if err := registry.Register("admin", valid); err == nil {
		t.Fatal("首次解析后不应允许修改注册表")
	}
	if _, err := registry.Resolve("missing"); err == nil {
		t.Fatal("未知方案应报错")
	}
}

func TestBearerTokenRejectsAmbiguousOrAlternateCredentials(t *testing.T) {
	for _, tc := range []struct {
		headers []string
		want    string
	}{
		{nil, ""},
		{[]string{"Bearer token"}, "token"},
		{[]string{"bEaReR\t token  "}, "token"},
		{[]string{"Bearer"}, ""},
		{[]string{"Basic token"}, ""},
		{[]string{"Bearer token extra"}, ""},
		{[]string{"Bearer token", "Bearer second"}, ""},
		{[]string{"Bearer token, Bearer second"}, ""},
	} {
		r := httptest.NewRequest(http.MethodGet, "/?token=query-token", nil)
		r.AddCookie(&http.Cookie{Name: "token", Value: "cookie-token"})
		for _, header := range tc.headers {
			r.Header.Add("Authorization", header)
		}
		got, err := BearerToken(r)
		if got != tc.want || (tc.want == "" && !errors.Is(err, ErrUnauthenticated)) || (tc.want != "" && err != nil) {
			t.Fatalf("headers=%v, token=%q, err=%v", tc.headers, got, err)
		}
	}
}

func TestPrincipalContextCopiesIdentityAndPreservesParent(t *testing.T) {
	type parentKey struct{}
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), parentKey{}, "kept"))
	defer cancel()
	principal := Principal{UserID: "42"}
	ctx := WithPrincipal(parent, "admin", principal)
	principal.UserID = "changed"
	read, ok := PrincipalFromContext(ctx)
	if !ok || read.UserID != "42" {
		t.Fatalf("身份未按值存储: %+v, %v", read, ok)
	}
	read.UserID = "mutated"
	read, _ = PrincipalFromContext(ctx)
	scheme, ok := SchemeFromContext(ctx)
	if read.UserID != "42" || !ok || scheme != "admin" || ctx.Value(parentKey{}) != "kept" {
		t.Fatalf("Context 信息错误: %+v, %q", read, scheme)
	}
	if _, ok := PrincipalFromContext(parent); ok {
		t.Fatal("不应修改父 Context")
	}
	if _, ok := SchemeFromContext(parent); ok {
		t.Fatal("父 Context 不应存在方案")
	}
	cancel()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("取消未传递")
	}
}

type testAuthenticator struct{}

func (*testAuthenticator) Authenticate(*http.Request) (*Principal, error) {
	return nil, ErrUnauthenticated
}
