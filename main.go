package main

import (
	"archive/zip"
	"fmt"
	"io"
	"os"

	simplejson "github.com/bitly/go-simplejson"
	"github.com/gin-gonic/gin"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"encoding/csv"
)

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
		err, convertedJson := convertBsonToSimpleJSON(result)
		chkFatal(err)
		c.JSON(200, convertedJson)
		// c.String(200, result)
	})

	r.GET("/job/:id/export", func(c *gin.Context) {
		id := bson.ObjectIdHex(c.Param("id"))
		var result bson.M
		err := jobCollection.Find(bson.M{"_id": id}).One(&result)
		err, convertedJson := convertBsonToSimpleJSON(result)
		chkFatal(err)
		bucket := convertedJson.GetPath("definition", "bucket").MustString("")
		storageKey := convertedJson.GetPath("definition", "storageKey").MustString("")
		if bucket == "" || storageKey == "" {
			return
		}
		exportReportString := GetSingleFileInFolder(bucket, storageKey+"/output/ExportReport", "json")
		exportReportJson, err := simplejson.NewJson([]byte(exportReportString))
		fmt.Println(exportReportString)
		chkFatal(err)
		c.Stream(func(responseWriter io.Writer) bool {
			outputTables := exportReportJson.Get("outputTables").MustArray()
			w := zip.NewWriter(responseWriter)
			for _, table := range outputTables {
				tableJSON := convertMapStringInterfaceBackToSimpleJson(table.(map[string]interface{}))
				tableName := tableJSON.Get("tableName").MustString()
				tableHeader := tableJSON.Get("columns").MustStringArray()
				fmt.Println("processing " + tableName)
				f, err := w.Create(tableName + ".csv")
				headerWriter := csv.NewWriter(f)
				headerWriter.Write(tableHeader)
				headerWriter.Flush()
				chkFatal(err)
				keys := GetAllFilesWithSuffix(bucket, storageKey+"/output/"+tableName+"/", ".csv.gz")
				err = GetDownloadStream(bucket, keys, f)
				chkFatal(err)
			}
			w.Close()
			return false
		})
		// keys := GetAllFilesWithSuffix(bucket, storageKey+"/output/Sample", ".csv.gz")
		// c.String(200, "ok")
		// c.Header("Content-Disposition", "attachment; filename=Sample.csv")
		// c.Stream(func(w io.Writer) bool {
		// 	err := GetDownloadStream(bucket, keys, w)
		// 	chkFatal(err)
		// 	// for _, element := range keys {
		// 	// 	//fmt.Println("adding " + element)
		// 	// 	err := GetDownloadStream(bucket, keys, w)
		// 	// 	chkFatal(err)
		// 	// }
		// 	return false
		// })
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

func convertBsonToSimpleJSON(input bson.M) (err error, result *simplejson.Json) {
	bytes, err := bson.MarshalJSON(input)
	if err != nil {
		return err, simplejson.New()
	}
	ret, err := simplejson.NewJson(bytes)
	chkFatal(err)
	return nil, ret
}

func convertMapStringInterfaceBackToSimpleJson(input map[string]interface{}) (output *simplejson.Json) {
	output = simplejson.New()
	for k, v := range input {
		output.Set(k, v)
	}
	return
}

func chkFatal(err error) {
	if err != nil {
		fmt.Println(err)
	}
}
