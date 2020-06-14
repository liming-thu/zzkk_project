package main
import (
	"context"
	"fmt"
	"gopkg.in/olivere/elastic.v6"
	_"github.com/go-sql-driver/mysql"
	"strings"
	"io/ioutil"
	"encoding/json"
	"database/sql"
	"sync"
)

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

func createIndex(indexName string, EsClient *elastic.Client){
	exists, err := EsClient.IndexExists(indexName).Do(context.Background())
	if err != nil {
		panic(err)
	}
	if !exists {
		_,err := EsClient.CreateIndex(indexName).BodyString(mapping).Do(context.Background())
		if err != nil {
			panic(err)
		} else {
			fmt.Println("Index created:",indexName)
		}
	}
}
func createAllIndexes(EsClient *elastic.Client, DbCon *sql.DB){
	//first remove all the indexes
	EsClient.DeleteIndex("*").Do(context.Background())
	//get all the doc types from mysql
	sqlStr := "select distinct Type from t_file_in_project"
	rows, err := DbCon.Query(sqlStr)
	var langType string
	if err!=nil{
		fmt.Println("Retrieving lang types error: ",err)
	} else {
		for rows.Next(){
			rows.Scan(&langType)
			langType=strings.ReplaceAll(langType,"_","")
			createIndex(langType,EsClient)
			}
	}
}
func ingetSingle(filePath string, fileName string, fileType string, EsClient *elastic.Client){
	src,err := ioutil.ReadFile(RootPath+filePath+fileName)
	if err == nil{
		item := SrcCodeDoc {Code: string(src),Size: len(src)}
		_,err := EsClient.Index().
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
func ingest(EsClient *elastic.Client, DbCon *sql.DB){
	bkRequest := EsClient.Bulk()
	//
	sql := "select distinct id as name,path,type from t_file,t_file_in_project where id=fileid and t_file.linenumber>0"	
	rows, err := DbCon.Query(sql)
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
			src,err := ioutil.ReadFile(RootPath+filePath)
			if err == nil{
				item := SrcCodeDoc {Code: string(src),Size: len(src)}
				indexReq := elastic.NewBulkIndexRequest().
				Index(fileType).
				Type("doc").
				Id(fileName).
				Version(1).//external version is used to control the duplication of files
				VersionType("external").//使用外部版本号是为了解决多次插入同一文件时的重复问题.
				Doc(item)
				bkRequest = bkRequest.Add(indexReq)
				i++
			} else {
				fmt.Println("Read file ERROR:", RootPath+filePath)
			}
			if i >= EsIngestBulkSize{
				_,err := bkRequest.Do(context.Background())
				if err!= nil {
					fmt.Println("Ingestion error", bkRequest.Pretty)
					panic(err)
				} else {
					i = 0
					fmt.Println(EsIngestBulkSize," documents ingested")
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
func SearchDoc(filePath string, fileName string, fileType string, EsClient *elastic.Client)([]EsResult){
	doc, err := ioutil.ReadFile(filePath)
	var docRes []EsResult
	if err != nil{
		fmt.Println("File Not Found!",filePath)
		return docRes
	}
	testDoc := string(doc)
	mlt := elastic.NewMoreLikeThisQuery().Field("Code").LikeText(testDoc)
	result, err := EsClient.Search().
	Index(fileType).
	Query(mlt).
	From(0).
	Size(EsSimFileNum).//top n files
	Pretty(true).
	Do(context.Background())
	if err!=nil{
		fmt.Println("Search Doc in ES error.")
		fmt.Println(err)
	}
	//append all the results
	for _,res := range result.Hits.Hits{
		docRes = append(docRes,EsResult{RefFileID:res.Id,TestFileID:fileName,Score:*res.Score})
	}
	//
	return docRes

}
//IsDocExists is ...
func IsDocExists(id string,filetype string, EsClient *elastic.Client){
	EsClient.Exists().Id(id)
}
//GetDocByID is the function to get the src code document by its file ID.
func GetDocByID(id string,filetype string, EsClient *elastic.Client) (string, int){
	result, err := EsClient.Get().Index(filetype).Id(id).Do(context.Background())
	if err != nil{
		fmt.Println(err)
		return "NULL", 0
	}
	if result.Found {
		fmt.Println(result.Id,*result.Version,result.Index,result.Type)
		var item SrcCodeDoc
		buf,_:= result.Source.MarshalJSON()
		json.Unmarshal(buf, &item)
		return item.Code, item.Size
	}
	return "NULL", 0
}


//EsDocIngestInfoCh is the channel to transfer EsDocInfos to routines
var EsDocIngestInfoCh = make(chan EsDocIngestInfo, EsRN)

// GetTestFiles is the ...
func GetTestFiles(DbCon *sql.DB) []EsDocIngestInfo{
	var files []EsDocIngestInfo
	sql := "select tm.id,tm.type,t.pathinproject from test_file_in_project t, test_file_sha_missed tm where t.fileid=tm.id"
	rows, err := DbCon.Query(sql)
	defer rows.Close()
	var fileName string
	var filePath string
	var fileType string
	if err!=nil{
		fmt.Println("Retriving file error from db: ",err)
	} else {
		for rows.Next(){
			rows.Scan(&fileName, &fileType, &filePath)
			file :=EsDocIngestInfo{path:RootPath+filePath,name:fileName,lang:fileType}
			files = append(files, file)
		}
	}
	return files
}

//SearchDocSingleRoutine used to search a batch of files
func SearchDocSingleRoutine(EsClient *elastic.Client, DcCon *sql.DB){
	files:=GetTestFiles(DcCon)
	for _,file := range files{
			SearchDoc(file.path,file.name,file.lang,EsClient)
	}
}
//EsResult is ...
type EsResult struct{
	RefFileID string
	TestFileID string
	Score float64
}

//EsResults is the list to collect all the results
var EsResults []EsResult

//ResultMutex is to sync the appending of results
var ResultMutex = &	sync.Mutex{}

// SearchDocMultiRoutine is ...
func SearchDocMultiRoutine(EsClient *elastic.Client, DbCon *sql.DB){
	for i:=0;i<=EsRN;i++{
		go Search(EsClient)
	}
	for _,file := range GetTestFiles(DbCon){
		EsDocIngestInfoCh<-file
	}
	close(EsDocIngestInfoCh)
	for i:=0;i<=EsRN;i++{
		<-SyncCh
	}

	//store results to db
	SaveEsResultsToDB(DbCon)
}
//SaveEsResultsToDB is ..
func SaveEsResultsToDB(DbCon *sql.DB){
	sql := "truncate test_result_es"
	DbCon.Exec(sql)
	//
	valueStrings := []string{}
	valueArgs := []interface{}{}
	
	tx,err := DbCon.Begin()
	if err != nil {
		return
	}

	i:=0 
	smt := `INSERT ignore INTO test_result_es(RefFileId, TestFileId, Score) values %s`

 	for _, r := range EsResults {
    	valueStrings = append(valueStrings, "(?, ?, ?)")
    	valueArgs = append(valueArgs, r.RefFileID)
    	valueArgs = append(valueArgs, r.TestFileID)
    	valueArgs = append(valueArgs, r.Score)
		i++
		if i%DbIngestBulkSize == 0{
			smt = fmt.Sprintf(smt, strings.Join(valueStrings, ","))
			_,txerr:= tx.Exec(smt, valueArgs...)
			if txerr!=nil{
				tx.Rollback()
				fmt.Println(txerr)
				return
			}
			valueStrings = []string{}
			valueArgs = []interface{}{}
			smt = `INSERT ignore INTO test_result_es(RefFileId, TestFileId, Score) values %s`
		}
	 }

	 //last results
	smt = fmt.Sprintf(smt, strings.Join(valueStrings, ","))
	_,txerr:= tx.Exec(smt, valueArgs...)
	if txerr!=nil{
		tx.Rollback()
		fmt.Println("Save ES search result to Database error!")
		fmt.Println(txerr)
		return
	}
	cmterr:=tx.Commit()
	if cmterr!=nil{
		fmt.Println("Save ES search result to Database error!")
		fmt.Println(cmterr)
	}

}
//Search is to search the files in batch
func Search(EsClient *elastic.Client){
	for {
		file, ok := <- EsDocIngestInfoCh
		if !ok{
			SyncCh <- false
			break
		}
		res:=SearchDoc(file.path,file.name,file.lang,EsClient)
		ResultMutex.Lock()
		EsResults=append(EsResults,res...)
		ResultMutex.Unlock()
	}
}