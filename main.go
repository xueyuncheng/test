package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"
)

var zapper *zap.Logger

func main() {
	configFile := flag.String("c", "./application.toml", "配置文件路径")

	flag.Parse()

	var err error
	zapper, err = zap.NewDevelopment()
	if err != nil {
		fmt.Println("日志启动失败")
		os.Exit(1)
	}

	config := &Config{}
	if _, err := toml.DecodeFile(*configFile, config); err != nil {
		zapper.Fatal("配置文件解析错误", zap.Error(err))
	}

	cfg := canal.NewDefaultConfig()
	cfg.Addr = net.JoinHostPort(config.Host, config.Port)
	cfg.User = config.User
	cfg.Password = config.Password
	cfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(cfg)
	if err != nil {
		zapper.Fatal("cannal初始化错误", zap.Error(err))
	}

	c.SetEventHandler(&handler{})

	startPos, err := c.GetMasterPos()
	startPos = mysql.Position{
		Name: "binlog.000004",
		Pos:  1693,
	}

	if err != nil {
		zapper.Error("获取对方的最新的binglog位置错误", zap.Error(err))
	}

	if err := c.RunFrom(startPos); err != nil {
		zapper.Fatal("canal 启动错误", zap.Error(err))
	}
}
