package main 
import "fmt"
import "strconv"
import "strings"
import "time"
import _"github.com/go-sql-driver/mysql"
import	"database/sql"

//FileLsh is ...
type FileLsh struct{
	TFileID string
	TestFileID string
	TLsh string
	TestLsh string
	HashTypeID string
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
    t_file_in_project.FileId, 
    test_file_in_project.FileId,
	t_lsh.Value,
	test_lsh.Value,
    test_lsh.HashTypeId
from 
    t_file_in_project, 
    test_file_in_project,
    t_lsh,
    test_lsh
where 
    test_file_in_project.ProjectId='%s' and
    t_file_in_project.ProjectId='%s' and 
    test_file_in_project.Name=t_file_in_project.Name and 
    test_file_in_project.Type=t_file_in_project.Type and 
    test_file_in_project.FileId <> t_file_in_project.FileId and
    test_lsh.HashTypeId=t_lsh.HashTypeId and
    test_lsh.FileId=test_file_in_project.FileId and 
	t_lsh.FileId=t_file_in_project.FileId`,TestProjectID,RefProjectID)
	//
	rows, err := DbCon.Query(sql)
	//
	if err!=nil{
		fmt.Println("Retrieving LSH error: ",err)
	} else {
		var tID string
		var testID string
		var tLsh string
		var testLsh string
		var hash string
		for rows.Next(){
			rows.Scan(&tID, &testID, &tLsh, &testLsh, &hash)
			FileCh <- FileLsh{TFileID:tID,TestFileID:testID,TLsh:tLsh,TestLsh:testLsh,HashTypeID:hash}
			}
	}

}

func main(){

	InitializeDb()
	
	startTime := time.Now()

	TestProjectID := "e75eb072955ab1ac9cab9a39a8815ba6003807e5"
	RefProjectIDs :=GetRefProjectID(TestProjectID)

	for i := 1; i<RN; i++ {
		go cmp(i)
	}

	for _,RefProjectID := range RefProjectIDs{
		GetLsh(TestProjectID,RefProjectID)
	}

	close(FileCh)
	for i := 1; i<RN; i++ {
		<-SyncCh
	}

	fmt.Println("TIME USED (ms): "+ strconv.FormatInt(time.Since(startTime).Milliseconds(),10))
}

func cmp(routineID int){
	for {
		Lsh,ok := <-FileCh
		if !ok{
			SyncCh <- true
			break
		}
		RefSet := make(map[string]bool)
		strs := strings.Split(Lsh.TLsh, ",")
		for _,str := range strs{ //construct the reference set/map
			RefSet[str] = true
		}
		//To be refined ++
		TestSet :=make(map[string]bool)
		strs = strings.Split(Lsh.TestLsh, ",")
		for _,str := range strs{ //construct the test set/map
			TestSet[str] = true
		}
		//To be refined --

		//intersection of test set and reference set
		intersectionLen := 0.0
		for k := range TestSet{
			if RefSet[k] == true{
				intersectionLen++
			}
		}
		fmt.Println(Lsh.TestFileID,Lsh.TFileID,Lsh.HashTypeID,intersectionLen/float64(len(TestSet)))
	}
}