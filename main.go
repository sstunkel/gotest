package main

import (
	"fmt"

	"github.com/gin-gonic/gin"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var DB = make(map[string]string)

type Job struct {
	// ApplicationID string
	// Created       string
	// Definition    struct {
	// 	ProjectUID            int
	// 	ProjectName           string
	// 	ProjectRevisionNumber int
	// 	Debug                 bool
	// }
	// JobType    string
	// ResourceID string
	// Status     string
	// User       string
	// V          int           `json:"__v"`
	_id bson.ObjectId `bson:"_id,omitempty"`
}

func main() {
	// Disable Console Color
	// gin.DisableConsoleColor()
	r := gin.Default()
	session, err := mgo.Dial("localhost:27017")
	jobCollection := session.DB("job-service").C("jobs")
	chkFatal(err)

	// Ping test
	r.GET("/ping", func(c *gin.Context) {
		c.String(200, "pong")
	})

	r.GET("/job/:id", func(c *gin.Context) {
		id := bson.ObjectIdHex(c.Param("id"))
		var result bson.M
		err := jobCollection.Find(bson.M{"_id": id}).One(&result)
		chkFatal(err)
		fmt.Println(result)
		var job Job = Job(result)
		fmt.Println(job)
		c.JSON(200, result)
		// c.String(200, result)
	})

	// // Get user value
	// r.GET("/user/:name", func(c *gin.Context) {
	// 	user := c.Params.ByName("name")
	// 	value, ok := DB[user]
	// 	if ok {
	// 		c.JSON(200, gin.H{"user": user, "value": value})
	// 	} else {
	// 		c.JSON(200, gin.H{"user": user, "status": "no value"})
	// 	}
	// })

	// // Authorized group (uses gin.BasicAuth() middleware)
	// // Same than:
	// // authorized := r.Group("/")
	// // authorized.Use(gin.BasicAuth(gin.Credentials{
	// //	  "foo":  "bar",
	// //	  "manu": "123",
	// //}))
	// authorized := r.Group("/", gin.BasicAuth(gin.Accounts{
	// 	"foo":  "bar", // user:foo password:bar
	// 	"manu": "123", // user:manu password:123
	// }))

	// authorized.POST("admin", func(c *gin.Context) {
	// 	user := c.MustGet(gin.AuthUserKey).(string)

	// 	// Parse JSON
	// 	var json struct {
	// 		Value string `json:"value" binding:"required"`
	// 	}

	// 	if c.Bind(&json) == nil {
	// 		DB[user] = json.Value
	// 		c.JSON(200, gin.H{"status": "ok"})
	// 	}
	// })

	// Listen and Server in 0.0.0.0:8080
	r.Run(":8080")
}

func chkFatal(err error) {
	if err != nil {
		fmt.Println(err)
	}
}
