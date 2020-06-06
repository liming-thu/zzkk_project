package main 
import "fmt"
//import "strconv"
import "strings"
//import "time"
import _"github.com/go-sql-driver/mysql"
import	"database/sql"

//FileLsh is ...
type FileLsh struct{
	FileID string
	HashTypeID string
	Value string
	SrcFileID string
}

//RN is the number of routines
var RN = 128

//FileCh is the channel to push files
var FileCh = make(chan FileLsh,200)

//SyncCh is the channel to sync routines
var SyncCh = make(chan bool, RN)

//ConStr is the connection string of mysql database
var ConStr = "root:liming@tcp(localhost:3306)/zzkk_lite"

//DbCon is the connection of mysql
var DbCon *sql.DB

// InitializeDb is to initialize database
func InitializeDb(){
	var err error
	DbCon, err = sql.Open("mysql", ConStr)
	if err != nil{
		panic(err)
	}
}

//GetRefProjectID is the function to get all the projects to be searched
func GetRefProjectID(TestProjectID string) []string{
	sql := "select distinct RefProjectId from test_result_project where TestProjectId='"+TestProjectID+"'"
	rows, err := DbCon.Query(sql)
	var res []string
	if err!=nil{
		fmt.Println("Retrieving RefProjectId error: ",err)
	} else {
		var ID string
		for rows.Next(){
			rows.Scan(&ID)
			res = append(res,ID)
			}
	}
	return res
}
//GetLsh is the function to get all the lsh of test and ref files
func GetLsh(TestProjectID string, RefProjectID string){
	sql :=fmt.Sprintf(`select 
    t.FileId, 
	t.HashTypeId,
	t.Value,
	sfsm.id
from 
    t_file_in_project fp, 
    t_lsh t,
    test_file_sha_missed sfsm
where 
    fp.ProjectId='%s' and 
    sfsm.Name=fp.Name and 
    sfsm.Type=fp.Type and 
	t.FileId=fp.FileId`,RefProjectID)
	//
	//fmt.Println(sql)
	//
	rows, err := DbCon.Query(sql)
	//
	if err!=nil{
		fmt.Println("Retrieving LSH error: ",err)
	} else {
		var id string
		var hash string
		var val string
		var sid string
		for rows.Next(){
			rows.Scan(&id, &hash,&val, &sid)
			FileCh <- FileLsh{FileID:id,HashTypeID:hash,Value:val,SrcFileID:sid}
			}
	}

}
//TestFileLshMap is the map of map of map of LSH: file->hashtype->lsh
var TestFileLshMap = make(map[string]map[string]map[string]bool)

//GetTestFiles is the function to get all the files and lsh to be tested.
func GetTestFiles(){
	//ID, HashTypeId, Value
	sql := "select test_lsh.* from test_lsh, test_file_sha_missed tfsm where tfsm.id = test_lsh.fileid"
	rows, err := DbCon.Query(sql)
	if err!=nil{
		fmt.Println("Retrieving RefProjectId error: ",err)
	} else {
		var ID string
		var HashTypeID string
		var Value string
		for rows.Next(){
			rows.Scan(&ID, &HashTypeID, &Value)
			lshmap := make(map[string]bool)
			strs := strings.Split(Value, ",")
			for _,str := range strs{ //construct the test set/map
				lshmap[str] = true
			}
			//
			if _,ok := TestFileLshMap[ID]; !ok{
				hashTypeMap := make(map[string]map[string]bool)
				hashTypeMap[HashTypeID]=lshmap
				TestFileLshMap[ID]=hashTypeMap
			} else{
				TestFileLshMap[ID][HashTypeID]=lshmap
			}
		}
	}
}

func main(){

	InitializeDb()
	
	

	//startTime := time.Now()
	GetTestFiles()//

	TestProjectID := "e75eb072955ab1ac9cab9a39a8815ba6003807e5"
	RefProjectIDs :=GetRefProjectID(TestProjectID)

	for i := 1; i<RN; i++ {
		go cmp(i)
	}

	for _,RefProjectID := range RefProjectIDs{
		//fmt.Println("Compare Project:", RefProjectID)
		GetLsh(TestProjectID,RefProjectID)
	}

	close(FileCh)
	for i := 1; i<RN; i++ {
		<-SyncCh
	}

	//fmt.Println("TIME USED (ms): "+ strconv.FormatInt(time.Since(startTime).Milliseconds(),10))
}

func cmp(routineID int){
	for {
		Lsh,ok := <-FileCh
		if !ok{
			SyncCh <- true
			break
		}
		RefSet := make(map[string]bool)
		strs := strings.Split(Lsh.Value, ",")
		for _,str := range strs{ //construct the reference set/map
			RefSet[str] = true
		}
		//intersection of test set and reference set
		intersectionLen := 0.0
		if lshMap,ok := TestFileLshMap[Lsh.SrcFileID][Lsh.HashTypeID]; ok{
			for k := range lshMap{
				if RefSet[k] == true{
					intersectionLen++
				}
			}
			fmt.Println(Lsh.SrcFileID,Lsh.FileID,Lsh.HashTypeID,intersectionLen/float64(len(lshMap)))
		}
	}
}