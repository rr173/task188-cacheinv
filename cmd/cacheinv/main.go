// Command cacheinv 是分布式缓存失效收敛验证服务入口。
//
// 支持 --addr（HTTP 监听地址）、--db（SQLite 路径）与 --smoke-test（离线自检）：
//   - 不带 --smoke-test：启动长驻 HTTP 服务；
//   - 带 --smoke-test：真实建库、构造拓扑/协议/场景、回放并验证收敛，
//     随后关闭并重开同一数据库验证持久化恢复，以 0/非 0 退出码结束。
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"task188-cacheinv/internal/httpapi"
	"task188-cacheinv/internal/selfcheck"
	"task188-cacheinv/internal/service"
	"task188-cacheinv/internal/store"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dbPath := flag.String("db", "cacheinv.db", "SQLite database file path")
	smoke := flag.Bool("smoke-test", false, "run offline smoke test and exit")
	flag.Parse()

	st, err := store.Open(store.Options{Path: *dbPath})
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	svc := service.New(st, time.Now)
	api := httpapi.New(svc)

	if *smoke {
		if err := selfcheck.Run(st, svc, *dbPath); err != nil {
			log.Fatalf("smoke test failed: %v", err)
		}
		log.Printf("smoke test passed (db=%s)", *dbPath)
		return
	}

	mux := http.NewServeMux()
	mux.Handle("/", api.Routes())
	log.Printf("task188-cacheinv listening on %s (db=%s)", *addr, *dbPath)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("http server: %v", err)
	}
}
