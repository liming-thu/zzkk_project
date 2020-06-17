package main
import (
	"fmt"
	"gopkg.in/olivere/elastic.v6"
	_"github.com/go-sql-driver/mysql"
	"database/sql"
	"time"
	"strconv"
	"os"
)

/////////////////////////GLOBAL VARS//////////////////////////////////

//RootPath is the path all src files stored
var RootPath="/home/liming/xbc_file_repos/"

//EsURL is the url for es
var EsURL = "http://192.168.1.127:9200"

//ConStr is the connection string of mysql database
var ConStr = "root:liming@tcp(192.168.1.127:3306)/zzkk_lite"

//CPUIntenseRN is the number of CPU intensivie routines
var CPUIntenseRN = 128

//EsRN is the number of routines of searching ES
var EsRN = 128

//SyncCh is the channel to sync routines
var SyncCh = make(chan bool, 128)

//EsIngestBulkSize is the bulkes ingest buld size
var EsIngestBulkSize = 1000

//DbIngestBulkSize is the record number when ingesting data to database, maximum = 10,000
var DbIngestBulkSize = 10000

//EsSimFileNum is the top n files similar to the test file
var EsSimFileNum = 5

//SrcCodeDoc is the struct of a source code document structure
type SrcCodeDoc struct{
	Code string `json: "code"`
	Size int `json: "size"`
}
//EsDocIngestInfo is the stucture used to ingest code source files to ES.
type EsDocIngestInfo struct{
	path string
	name string
	lang string
}
//FileLshDb is ...
type FileLshDb struct{
	RefFileID string
	HashTypeID string
	Value string
	TestFileID string
}

//////////////////////////////////////////////////////////////////////////

//InitializeES is ...
func InitializeES(url string) *elastic.Client{
	client, err := elastic.NewClient(elastic.SetURL(url))
	if err!=nil{
		panic(err)
	}
	return client
}
//InitializeDB is ...
func InitializeDB(conStr string)*sql.DB{
	dbCon, err := sql.Open("mysql", conStr)
	if err != nil{
		panic(err)
	}
	return dbCon
}

//PrintTime is ...
func PrintTime(msg string){
	fmt.Println(msg,time.Now().Format("15:04:05.000"))
}
/****************************************************************************/

func main(){

	if len(os.Args)>1{
		RootPath=os.Args[1]
		_,err := os.Stat(RootPath)
		if err!=nil{
			fmt.Println("File path error!")
			return
		}
	}

	//DbCon is the connection of mysql
	// DbCon := InitializeDB(ConStr)

	// //EsClient is the client of ES
	// EsClient := InitializeES(EsURL)

	ts := time.Now();

	// SearchDocMultiRoutine(EsClient,DbCon)//ES Search

	// fmt.Println("ES time used:",strconv.FormatInt(time.Since(ts).Milliseconds(),10))

	// CmpTestFileLshFromEs(DbCon)// LSH compare

	fmt.Println("Total time used:",strconv.FormatInt(time.Since(ts).Milliseconds(),10))




	CreateWordReport("report.docx")
}