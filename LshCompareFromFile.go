package main 
import "fmt"
import "os"
import "io/ioutil"
import "strings"
import "database/sql"


//TestFileLshMapFile is the set to be searched in the files
var TestFileLshMapFile = make(map[string]map[string]map[string]bool)
//LSHChFile is ...
var LSHChFile = make(chan FileLshDb,CPUIntenseRN)

//CmpFileTestMain is 
func CmpFileTestMain(DbCon *sql.DB){
	
	for i := 1; i<CPUIntenseRN; i++ {
		go CompareLshFromFile()
	}

	//Get the lsh of test file and map them into memory
	GetTestFileLshFromFile(DbCon)

	//Get all the lsh of ref files from files
	GetRefIDFromDB(DbCon)

	for i := 1; i<CPUIntenseRN; i++ {
		<-SyncCh
	}
}
//ExportRefLshFromDB is ...
func ExportRefLshFromDB(DbCon *sql.DB){
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
			os.MkdirAll("/home/liming/Lsh/"+hash+"/",os.ModePerm)
			f,err := os.Create("/home/liming/Lsh/"+hash+"/"+refid)
			if err!=nil{
				fmt.Println("Creating file error:",err)
				return
			}
			f.WriteString(val)
			f.Close()
		}
	}
}
//ExportTestLshFromDB is ..
func ExportTestLshFromDB(DbCon *sql.DB){
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
			os.MkdirAll("/home/liming/Lsh/"+HashTypeID+"/",os.ModePerm)
			f,err := os.Create("/home/liming/Lsh/"+HashTypeID+"/"+ID)
			if err!=nil{
				fmt.Println("Creating file error:",err)
				return
			}
			f.WriteString(Value)
			f.Close()
		}
	}
}
//GetTestFileLshFromFile is the function to get all the files and lsh to be tested.
func GetTestFileLshFromFile(DbCon *sql.DB){
	//ID, HashTypeId, Value
	sql := "select test_lsh.FileId,test_lsh.HashTypeId from test_lsh, test_file_sha_missed tfsm where tfsm.id = test_lsh.fileid"
	rows, err := DbCon.Query(sql)
	if err!=nil{
		fmt.Println("Retrieving RefProjectId error: ",err)
	} else {
		var ID string
		var HashTypeID string
		for rows.Next(){
			rows.Scan(&ID, &HashTypeID)
			lshmap := make(map[string]bool)
			Value,err := ioutil.ReadFile("/home/liming/Lsh/"+HashTypeID+"/"+ID)
			if err!=nil{
				fmt.Println("Read file error,", ID, err)
				continue
			}
			strs := strings.Split(string(Value), ",")
			for _,str := range strs{ //construct the test set/map
				lshmap[str] = true
			}
			//
			if _,ok := TestFileLshMapFile[ID]; !ok{
				hashTypeMap := make(map[string]map[string]bool)
				hashTypeMap[HashTypeID]=lshmap
				TestFileLshMapFile[ID]=hashTypeMap
			} else{
				TestFileLshMapFile[ID][HashTypeID]=lshmap
			}
		}
	}
}
//GetRefIDFromDB is ...
func GetRefIDFromDB(DbCon *sql.DB){
	sql := "select e.RefFileId,t.hashtypeid,e.testfileid from test_result_es e,t_lsh t where e.reffileid=t.fileid"
	rows, err := DbCon.Query(sql)
	defer rows.Close()
	if err!=nil{
		fmt.Println("Retrieving LSH error: ",err)
	} else {
		var refid string
		var hash string
		var testid string
		for rows.Next(){
			rows.Scan(&refid, &hash, &testid)
			LSHChFile <- FileLshDb{RefFileID:refid,HashTypeID:hash,Value:"",TestFileID:testid}
		}
		close(LSHChFile)
	}
}
//CompareLshFromFile is the latest function to compare test lsh and ref lsh. Test lsh is in memory, ref lsh is reading from file ...
func CompareLshFromFile(){
	for {
		val, ok := <-LSHChFile
		if !ok{
			SyncCh<-true
			break;
		}
		//
		content,_ := ioutil.ReadFile("/home/liming/"+val.HashTypeID+"/"+val.RefFileID)
		//
		lines := strings.Split(string(content), "\n")
		for _,line := range lines{
			intersectionLen := 0
			FileRefLshMap := make(map[string]bool)
			strs := strings.Split(line, ",")
			for _,str := range strs{ //construct the reference set/map
				FileRefLshMap[str] = true
			}
			//intersection of test set and reference set
			for k := range TestFileLshMapFile[val.RefFileID][val.HashTypeID]{
				if FileRefLshMap[k] == true{
					intersectionLen++
				}
			}
		}
	}
}