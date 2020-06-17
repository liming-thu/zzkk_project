package main 
import "fmt"
import "strings"
import _"github.com/go-sql-driver/mysql"
import "database/sql"
import "sync"

//LSHCh is the channel to send LSH values
var LSHCh = make(chan FileLshDb,CPUIntenseRN)
//GetRefProjectID is the function to get all the projects to be searched
func GetRefProjectID(TestProjectID string, DbCon *sql.DB) []string{
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
//GetRefLshFromEsResult is ...
func GetRefLshFromEsResult(DbCon *sql.DB){
	sql := "select e.RefFileId,t.hashtypeid,t.value,e.testfileid from test_result_es e,t_lsh t where e.reffileid=t.fileid"
	rows, err := DbCon.Query(sql)
	defer rows.Close()
	if err!=nil{
		fmt.Println("Retrieving LSH error: ",err)
	} else {
		var refid string
		var hash string
		var val string
		var testid string
		for rows.Next(){
			rows.Scan(&refid, &hash,&val, &testid)
			LSHCh <- FileLshDb{RefFileID:refid,HashTypeID:hash,Value:val,TestFileID:testid}
		}
	}

}

//GetRefLsh is the function to get all the lsh of test and ref files
func GetRefLsh(TestProjectID string, RefProjectID string, DbCon *sql.DB){
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
		var refid string
		var hash string
		var val string
		var testid string
		for rows.Next(){
			rows.Scan(&refid, &hash,&val, &testid)
			LSHCh <- FileLshDb{TestFileID:testid,HashTypeID:hash,Value:val,RefFileID:refid}
			}
	}

}
//CreateTestFileOfShaMissed is ...
func CreateTestFileOfShaMissed(DbCon *sql.DB){
	//Clear sha_missed table
	sql := "truncate test_file_sha_missed"
	_, err:= DbCon.Exec(sql)
	if err!=nil{
		panic(err)
	}
	//Ingest records into sha_missed table
	sql = "insert into test_file_sha_missed select t.id,tp.Name,tp.Type from test_file t,test_file_in_project tp where tp.fileid=t.id and t.id not in (select tr.testfileid from test_result_file tr where tr.hashtypeid='GitHashCompare') and t.id not in (select Id from test_file_abnormal)"
	_, err= DbCon.Exec(sql)
	if err!=nil{
		panic(err)
	}
}
//TestFileLshMap is the map of map of map of LSH: file->hashtype->lsh
var TestFileLshMap = make(map[string]map[string]map[string]bool)

//GetTestFileLsh is the function to get all the files and lsh to be tested.
func GetTestFileLsh(DbCon *sql.DB){
	//ID, HashTypeId, Value
	sql := "select test_lsh.FileId,test_lsh.HashTypeId,test_lsh.Value from test_lsh, test_file_sha_missed tfsm where tfsm.id = test_lsh.fileid"
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
//SaveLshResultsToDB is ...
func SaveLshResultsToDB(DbCon *sql.DB) {
	sql := "delete from test_result_file where hashtypeid like 'ES_%'"
	DbCon.Exec(sql)
	//
	valueStrings := []string{}
	valueArgs := []interface{}{}
	
	tx,err := DbCon.Begin()
	if err != nil {
		return
	}

	i:=0 
	smt := `INSERT ignore INTO test_result_file(TestFileId, HashTypeId, RefFileId, Result) values %s`

 	for _, r := range LshResults {
		if r.Result>0.5 {
			valueStrings = append(valueStrings, "(?, ?, ?, ?)")
			valueArgs = append(valueArgs, r.TestFileID)
			valueArgs = append(valueArgs, r.HashTypeID)
			valueArgs = append(valueArgs, r.RefFileID)
			valueArgs = append(valueArgs, r.Result)
			i++
			if i%DbIngestBulkSize == 0{
				smt = fmt.Sprintf(smt, strings.Join(valueStrings, ","))
				_,txerr:= tx.Exec(smt, valueArgs...)
				if txerr!=nil{
					tx.Rollback()
					fmt.Println("Save lsh compare result to Database error!",i)
					fmt.Println(txerr)
					return
				}
				valueStrings = []string{}
				valueArgs = []interface{}{}
				smt = `INSERT ignore INTO test_result_file(TestFileId, HashTypeId, RefFileId, Result) values %s`
			}
		}
	 }

	 //last results
	if len(valueArgs)>0 {
		smt = fmt.Sprintf(smt, strings.Join(valueStrings, ","))
		_,txerr:= tx.Exec(smt, valueArgs...)
		if txerr!=nil{
			tx.Rollback()
			fmt.Println("Save last lsh compare result to Database error!")
			fmt.Println(txerr)
			return
		}
	}
	cmterr:=tx.Commit()
	if cmterr!=nil{
		fmt.Println("Commit lsh compare result to Database error!")
	}

}
//CmpTestFileLshFromEs is ...
func CmpTestFileLshFromEs(DbCon *sql.DB){
	
	//PrintTime("Loading Test LSH")
	
	GetTestFileLsh(DbCon)// initialize test file lsh map

	//PrintTime("Loading Test LSH")
	//
	for i := 1; i<CPUIntenseRN; i++ {
		go LSHCmpDb(i)
	}

	GetRefLshFromEsResult(DbCon)

	close(LSHCh)
	
	for i := 1; i<CPUIntenseRN; i++ {
		<-SyncCh
	}
	//save to db
	SaveLshResultsToDB(DbCon)
}
//CmpTestFromProject is ...
func CmpTestFromProject(DbCon *sql.DB){

	//startTime := time.Now()
	GetTestFileLsh(DbCon)//

	TestProjectID := "e75eb072955ab1ac9cab9a39a8815ba6003807e5"
	RefProjectIDs :=GetRefProjectID(TestProjectID,DbCon)

	for i := 1; i<CPUIntenseRN; i++ {
		go LSHCmpDb(i)
	}

	for _,RefProjectID := range RefProjectIDs{
		//fmt.Println("Compare Project:", RefProjectID)
		GetRefLsh(TestProjectID,RefProjectID,DbCon)
	}

	close(LSHCh)
	for i := 1; i<CPUIntenseRN; i++ {
		<-SyncCh
	}

	//fmt.Println("TIME USED (ms): "+ strconv.FormatInt(time.Since(startTime).Milliseconds(),10))
}

//LshResult is ...
type LshResult struct{
	TestFileID string
	RefFileID string
	HashTypeID string
	Result float64
}

//LshResults is ...
var LshResults []LshResult

//LshResultsMutex is ...
var LshResultsMutex = & sync.Mutex{}

//LSHCmpDb is ...
func LSHCmpDb(routineID int){
	for {
		Lsh,ok := <-LSHCh
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
		if lshMap,ok := TestFileLshMap[Lsh.TestFileID][Lsh.HashTypeID]; ok{
			for k := range lshMap{
				if RefSet[k] == true{
					intersectionLen++
				}
			}
			LshResultsMutex.Lock()
			LshResults = append(LshResults, LshResult{TestFileID:Lsh.TestFileID, RefFileID:Lsh.RefFileID, HashTypeID:"ES_"+Lsh.HashTypeID,Result:intersectionLen/float64(len(lshMap))})
			LshResultsMutex.Unlock()
		}
	}
}