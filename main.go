package main

import (
	"github.com/HeRedBo/pkg/cache"
	"github.com/HeRedBo/pkg/es"
	"github.com/HeRedBo/pkg/shutdown"
	"github.com/go-redis/redis/v7"
	"github.com/gookit/goutil/dump"
	"go.uber.org/zap"
	"product-consumer/global"
	"product-consumer/internal/consumer"
)

func init() {
	global.LoadConfig()
	global.LOG = global.SetupLogger()
	initRedisClient()
	initESClient()
}

func initRedisClient() {
	redisCfg := global.CONFIG.Redis
	opt := redis.Options{
		Addr:        redisCfg.Host,
		Password:    redisCfg.Password,
		IdleTimeout: redisCfg.IdleTimeout,
	}
	err := cache.InitRedis(cache.DefaultRedisClient, &opt)
	if err != nil {
		global.LOG.Error("redis init error", zap.Error(err), "client", cache.DefaultRedisClient)
		panic("initRedisClient error")
	}
}

// 初始化ES
func initESClient() {
	err := es.InitClientWithOptions(es.DefaultClient, global.CONFIG.Elasticsearch.Hosts,
		global.CONFIG.Elasticsearch.Username,
		global.CONFIG.Elasticsearch.Password,
		es.WithScheme("https"))
	dump.P(err)
	if err != nil {
		global.LOG.Error("InitClientWithOptions error", err, "client", es.DefaultClient)
		panic(err)
	}
	global.ES = es.GetClient(es.DefaultClient)
}

func initMongoClient() {
	// TO DO ...
}

func main() {
	//开启消费者
	consumer.StartConsumer()

	//优雅关闭
	shutdown.NewHook().Close(
		func() {
			//kafka consumer
			dump.P("kafka consumer closed")
			consumer.CloseProductConsumer()
		},
		func() {
			//es
			dump.P("es closed")
			es.CloseAll()
		},
	)
}
