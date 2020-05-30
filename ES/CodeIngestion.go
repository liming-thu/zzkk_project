package main
import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
	_"github.com/go-sql-driver/mysql"
	"database/sql"
	"strings"
	"io/ioutil"
)
//CodeDoc is the struct of a source code document structure
type CodeDoc struct{
	code string `json: "code"`
	size int `json: "size"`
}
type myFile struct{
	path string
	name string
	lang string
}
var RootPath="/home/liming/xbc_file_repos/"
var conStr = "root:liming@tcp(localhost:3306)/zzkk_lite"
var dbCon *sql.DB
func initDB(){
	var err error
	dbCon, err = sql.Open("mysql", conStr)
	if err != nil{
		fmt.Println("Connecting to mysql failed: ",err)
	}
}
const mapping = `{
    "mappings": {
		"type_name":{
        "properties": {
            "code": {
                "type": "text"
            }
		}
	}
    }
}`

const url = "http://localhost:9200"
func createIndex(indexName string){
	ctx := context.Background()
	client, err:= elastic.NewClient(elastic.SetURL(url))
	if err != nil {
		panic(err)
	}
	exists, err := client.IndexExists(indexName).Do(ctx)
	if err != nil {
		panic(err)
	}
	if !exists {
		_,err := client.CreateIndex(indexName).BodyString(mapping).Do(ctx)
		if err != nil {
			panic(err)
		} else {
			fmt.Println("Index created:",indexName)
		}
	}
}
func createAllIndexes(){
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
func ingest(){
	//create client in each routine... just for test.
	ctx := context.Background()
	client, err:= elastic.NewClient(elastic.SetURL(url))
	if err != nil {
		panic(err)
	}
	bkRequest := client.Bulk()
	//begin ingestion
	for {
		mf, ok := <- fileCh
		if !ok{
			syncCh<-true
			break
		}
		//ingest to es
		for _,f := range mf{
			doc,_ := ioutil.ReadFile(f.path+f.name)
			indexReq := elastic.NewBulkIndexRequest().Index(f.lang).Id(f.name).Doc(string(doc))
			bkRequest = bkRequest.Add(indexReq)
		}
		_,err := bkRequest.Refresh("waite_for").Do(ctx)
		if err!= nil {
			panic(err)
		}
	}
}

var RN = 1
var bulkSize = 100
var fileCh = make(chan []myFile, 150)
var syncCh = make(chan bool, RN)

func main(){
	initDB()
	//createAllIndexes()
	sql := "select id as name,substring(path,1,15) as path,type from t_file,t_file_in_project where id=fileid"	
	rows, err := dbCon.Query(sql)
	var fileName string
	var filePath string
	var fileType string
	if err!=nil{
		fmt.Println("Retriving file error from db: ",err)
	} else {
		start ingest routine
		for i := 0; i<RN; i++{
			go ingest()
		}
		var tmpBulk []myFile
		for rows.Next(){
			rows.Scan(&fileName, &filePath, &fileType)
			if len(tmpBulk) < bulkSize {
				tmpBulk = append(tmpBulk, myFile{path:filePath,name:fileName,lang:fileType})
			} else {
				tmpBulk = nil
			}
		}
		//send the last items to channel
		if len(tmpBulk)>0{
			fileCh <- tmpBulk
		}
		close(fileCh)
		//wait till all the routines completed
		for i := 1; i<RN; i++ {
			<-syncCh
		}
	
	}
}