package main

import "gopkg.in/gin-contrib/cors.v1"
import "github.com/gin-gonic/gin"
import "github.com/garyburd/redigo/redis"
import "net/http"
import "log"
import "os"

type Result struct {
	UserID string `json:"uid" form:"uid" binding:"required"`
	Name   string `json:"name" form:"name" binding:"required"`
	Score  int    `json:"score" form:"score" binding:"required"`
	Rank   int    `json:"rank"`
}

func main() {
	redisPool := redis.NewPool(func() (redis.Conn, error) {
		c, err := redis.DialURL(os.Getenv("REDISTOGO_URL"))
		if err != nil {
			log.Println(err)
			return nil, err
		}
		return c, nil
	}, 10)

	defer redisPool.Close()

	router := gin.Default()

	router.Use(cors.Default())

	router.POST("/scores", addScore(redisPool))
	router.GET("/totals/:window", getTotals(redisPool))

	router.Run()
}

func addScore(pool *redis.Pool) func(*gin.Context) {
	return func(g *gin.Context) {
		c := pool.Get()
		defer c.Close()

		var result Result

		if g.Bind(&result) == nil {
			c.Send("MULTI")
			c.Send("HSET", "user:"+result.UserID, "name", result.Name)
			c.Send("ZINCRBY", "totals:all", result.Score, result.UserID)
			c.Flush()
			_, err := c.Do("EXEC")
			if err != nil {
				log.Println("Error storing result: ", result, err)
				g.AbortWithStatus(http.StatusInternalServerError)
				return
			}
			g.Status(http.StatusCreated)
		}
	}
}

func getTotals(pool *redis.Pool) func(*gin.Context) {
	return func(g *gin.Context) {
		c := pool.Get()
		defer c.Close()

		var results []Result

		window := g.Param("window")
		switch window {
		case "all":
			reply, err := redis.Values(c.Do("ZREVRANGE", "totals:all", 0, 9, "WITHSCORES"))
			if err != nil {
				log.Println(err)
				g.AbortWithStatus(http.StatusInternalServerError)
				return
			}

			results = make([]Result, len(reply)/2)
			c.Send("MULTI")

			for i := range results {
				reply, err = redis.Scan(reply, &results[i].UserID, &results[i].Score)
				c.Send("ZREVRANK", "totals:all", results[i].UserID)
				c.Send("HGET", "user:"+results[i].UserID, "name")
			}

			c.Flush()
			reply, err = redis.Values(c.Do("EXEC"))
			if err != nil {
				log.Println(err)
				g.AbortWithStatus(http.StatusInternalServerError)
				return
			}

			for i := range results {
				reply, err = redis.Scan(reply, &results[i].Rank, &results[i].Name)
			}

		default:
			g.AbortWithStatus(http.StatusNotImplemented)
			return
		}

		g.JSON(200, results)
	}
}
