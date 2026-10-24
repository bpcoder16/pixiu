package websocketx_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/bpcoder16/pixiu/infra/websocketx"
	"github.com/gorilla/websocket"
)

func ExampleNew() {
	endpoint, err := websocketx.New(websocketx.Config{
		MaxConnections: 100,
		OnMessage: func(ctx context.Context, session *websocketx.Session, message websocketx.Message) error {
			return session.Send(ctx, message)
		},
	})
	if err != nil {
		panic(err)
	}
	server := httptest.NewServer(endpoint)
	defer server.Close()
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	if err := client.WriteMessage(websocket.TextMessage, []byte("hello\nworld")); err != nil {
		panic(err)
	}
	_, text, err := client.ReadMessage()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%q\n", text)
	if err := client.WriteMessage(websocket.BinaryMessage, []byte{0, 255}); err != nil {
		panic(err)
	}
	_, binary, err := client.ReadMessage()
	if err != nil {
		panic(err)
	}
	fmt.Println(binary)
	_ = endpoint.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := endpoint.Wait(ctx); err != nil {
		endpoint.Cancel()
		panic(err)
	}
	// Output:
	// "hello\nworld"
	// [0 255]
}
