package rediscache

import (
	"testing"

	"github.com/betas-in/logger"
	"github.com/betas-in/utils"
)

func TestRedis(t *testing.T) {

	conf := Config{
		Host:     "127.0.0.1",
		Port:     9876,
		Password: "596a96cc7bf9108cd896f33c44aedc8a",
		DB:       0,
	}
	log := logger.NewLogger(0, true)

	c, err := NewCache(&conf, log)
	utils.Test().Nil(t, err)

	_ = c.GetClient()
	err = c.Close()
	utils.Test().Nil(t, err)
}
