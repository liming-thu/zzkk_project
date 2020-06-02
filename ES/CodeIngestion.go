package main
import (
	"context"
	"fmt"
	"gopkg.in/olivere/elastic.v6"
	_"github.com/go-sql-driver/mysql"
	"database/sql"
	"strings"
	"io/ioutil"
	"encoding/json"
	"strconv"
	"time"
)
//SrcCodeDoc is the struct of a source code document structure
type SrcCodeDoc struct{
	Code string `json: "code"`
	Size int `json: "size"`
}
type myFile struct{
	path string
	name string
	lang string
}
var RootPath="/home/liming/xbc_file_repos/"
var conStr = "root:liming@tcp(localhost:3306)/zzkk_lite"
var dbCon *sql.DB
var client *elastic.Client
var bulkSize = 1000
const url = "http://localhost:9200"

func initialize(){
	var err error
	dbCon, err = sql.Open("mysql", conStr)
	if err != nil{
		panic(err)
	}
	client, err = elastic.NewClient()
	if err!=nil{
		panic(err)
	}
}
const mapping =`{"mappings": {
    "doc": {
      "properties": {
        "Code": {
          "type": "text",
		  "term_vector": "yes"
        },
        "Size": {
          "type": "long"
        }
      }
    }
  }}`
func createIndex(indexName string){
	exists, err := client.IndexExists(indexName).Do(context.Background())
	if err != nil {
		panic(err)
	}
	if !exists {
		_,err := client.CreateIndex(indexName).BodyString(mapping).Do(context.Background())
		if err != nil {
			panic(err)
		} else {
			fmt.Println("Index created:",indexName)
		}
	}
}
func createAllIndexes(){
	//first remove all the indexes
	client.DeleteIndex("*").Do(context.Background())
	//get all the doc types from mysql
	sqlStr := "select distinct Type from t_file_in_project"
	rows, err := dbCon.Query(sqlStr)
	var langType string
	if err!=nil{
		fmt.Println("Retrieving lang types error: ",err)
	} else {
		for rows.Next(){
			rows.Scan(&langType)
			langType=strings.ReplaceAll(langType,"_","")
			createIndex(langType)
			}
	}
}
func ingetSingle(filePath string, fileName string, fileType string){
	src,err := ioutil.ReadFile(RootPath+filePath+fileName)
	if err == nil{
		item := SrcCodeDoc {Code: string(src),Size: len(src)}
		_,err := client.Index().
		Index(fileType).
		Type("doc").
		Id(fileName).
		BodyJson(item).
		Refresh("wait_for").
		Do(context.Background())
		if err!=nil{
			panic(err)
		} else {
			fmt.Println(fileName, "ingested")
		}

	} else {
		fmt.Println("Read file error:", RootPath+filePath+fileName)
		}
}
func ingest(){
	bkRequest := client.Bulk()
	//
	sql := "select distinct id as name,substring(path,1,15) as path,type from t_file,t_file_in_project where id=fileid"	
	rows, err := dbCon.Query(sql)
	var fileName string
	var filePath string
	var fileType string
	if err!=nil{
		fmt.Println("Retriving file error from db: ",err)
		panic(err)
	} else {
		i := 0
		for rows.Next(){
			rows.Scan(&fileName, &filePath, &fileType)			
			if i<bulkSize {
				src,err := ioutil.ReadFile(RootPath+filePath+fileName)
				if err == nil{
					item := SrcCodeDoc {Code: string(src),Size: len(src)}
					indexReq := elastic.NewBulkIndexRequest().
					Index(fileType).
					Type("doc").
					Id(fileName).
					Doc(item)
					bkRequest = bkRequest.Add(indexReq)
					i++
				} else {
					fmt.Println("Read file error:", RootPath+filePath+fileName)
				}
			}else{
				_,err := bkRequest.Do(context.Background())
				if err!= nil {
					panic(err)
				}
				if bkRequest.NumberOfActions() == 0{
					i = 0
					fmt.Println(bulkSize," documents ingested")
				}
			}
		}
		if bkRequest.NumberOfActions()>0{
			_,err := bkRequest.Do(context.Background())
			if err!= nil {
				panic(err)
			}
		}
	}
}
//SearchDoc is to ...
func SearchDoc(filePath string, fileName string, fileType string) (float64, string){
	doc, err := ioutil.ReadFile(filePath+fileName)
	if err != nil{
		panic(err)
	}
	testDoc := string(doc)

	mlt := elastic.NewMoreLikeThisQuery().Field("Code").LikeText(testDoc)
	result, err := client.Search().
	Index(fileType).
	Query(mlt).
	From(0).
	Size(10).
	Pretty(true).
	Do(context.Background())
	if err!=nil{
		panic(err)
	}
	for _,res := range result.Hits.Hits{
		fmt.Println(*res.Score,res.Id)
		return *res.Score,res.Id
	}
	return 0.0, ""

}

//GetDocById is the function to get the src code document by its file ID.
func GetDocById(id string) (string, int){
	result, err := client.Get().Index("haml").Id(id).Do(context.Background())
	if err != nil{
		fmt.Println(err)
	}
	if result.Found {
		fmt.Println(result.Id,result.Version,result.Index,result.Type)
		var item SrcCodeDoc
		buf,_:= result.Source.MarshalJSON()
		json.Unmarshal(buf, &item)
		return item.Code, item.Size
	}
	return "NULL", 0
}

//SearchDocBulk used to search a batch of files
func SearchDocBulk(fileNum int, topNum int){
	sql := "select distinct id as name,substring(path,1,15) as path,type from t_file,t_file_in_project where id=fileid limit "+ strconv.Itoa(fileNum)
	rows, err := dbCon.Query(sql)
	var fileName string
	var filePath string
	var fileType string
	if err!=nil{
		fmt.Println("Retriving file error from db: ",err)
		panic(err)
	} else {
		for rows.Next(){
			rows.Scan(&fileName, &filePath, &fileType)
			SearchDoc(RootPath+filePath,fileName,fileType)
		}
	}
}
//global vars of go routines
const RN = 100
var fileCh = make(chan myFile, 150)
var syncCh = make(chan bool, RN)
//
func MultiRoutine(fileNum int){
	for i:=0;i<=RN;i++{
		go Search()
	}
	sql := "select distinct id as name,substring(path,1,15) as path,type from t_file,t_file_in_project where id=fileid limit "+ strconv.Itoa(fileNum)
	rows, err := dbCon.Query(sql)
	var fileName string
	var filePath string
	var fileType string
	if err!=nil{
		fmt.Println("Retriving file error from db: ",err)
		panic(err)
	} else {
		for rows.Next(){
			rows.Scan(&fileName, &filePath, &fileType)
			file :=myFile{path:RootPath+filePath,name:fileName,lang:fileType}
			fileCh<-file
		}
		close(fileCh)
	}
	for i:=0;i<=RN;i++{
		<-syncCh
	}
}
//Search is to search the files in batch
func Search(){
	for {
		file, ok := <- fileCh
		if !ok{
			syncCh <- false
			break
		}
		SearchDoc(file.path,file.name,file.lang)
	}
}
func main(){
	initialize()
	// createAllIndexes()
	// ingest()	
	//SearchDocBulk(1000,5)
	stime := time.Now()
	MultiRoutine(1000)
	etime := time.Now()
	fmt.Println(etime.Sub(stime))
}