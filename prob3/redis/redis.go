package redis

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type redisgo struct {
	client *redis.Client
}

func New(addr, pass string) redisgo {
	var db redisgo
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr, //"localhost:6379"
		Password: pass, // no password set
		DB:       0,    // use default DB
	})
	db.client = rdb
	return db
}

func (rdb redisgo) Get(key string) string {
	val, err := rdb.client.Get(ctx, key).Result()
	if err == redis.Nil {
		fmt.Println("key does not exist")
	} else if err != nil {
		panic(err)
	} else {
		return val
	}
	return ""
}

func (rdb redisgo) GetAll() map[string]string {
	ctx := context.Background()
	result := make(map[string]string)
	var cursor uint64
	for {
		keys, nextCursor, err := rdb.client.Scan(ctx, cursor, "*", 0).Result()
		if err != nil {
			log.Fatalf("Error scanning keys: %v", err)
		}
		cursor = nextCursor
		for _, key := range keys {
			val := rdb.Get(key)
			result[key] = val
		}
		if cursor == 0 {
			break
		}
	}

	return result
}

func (rdb redisgo) Put(key, val string) error {

	err := rdb.client.Set(ctx, key, val, 0).Err()
	if err != nil {
		return err
	}
	return nil
}
