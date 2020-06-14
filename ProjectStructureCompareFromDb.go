package main
import (
 "fmt"
 "strconv"
 "strings"
 "time"
 _"github.com/go-sql-driver/mysql"
 "database/sql"
)


var structureCh = make(chan string, CPUIntenseRN)

//TestStructureMap is the map of ...
var TestStructureMap = make(map[string]bool) 

//ProjectStructureCompare is ...
func ProjectStructureCompare(DbCon *sql.DB){

	var testStructure string
	
	DbCon.QueryRow("select Structure from test_project").Scan(&testStructure)

	for i := 1; i<CPUIntenseRN; i++ {
		go CmpStructure(i)
	}

	for _,str := range strings.FieldsFunc(testStructure,Split){
		TestStructureMap[str] = true
	}

	tp,_ := DbCon.Query("select Structure from t_project")
	for tp.Next(){
		var p string
		tp.Scan(&p)
		structureCh<-p
	}

	startTime := time.Now()

	close(structureCh)
	for i := 1; i<CPUIntenseRN; i++ {
		<-SyncCh
	}

	fmt.Println("TIME USED (ms): "+ strconv.FormatInt(time.Since(startTime).Milliseconds(),10))
}
//CmpStructure is ...
func CmpStructure(routineID int){	
	for {
		structure, ok := <-structureCh
		if !ok{
			SyncCh<-true
			break;
		}
		lines := strings.Split(string(structure), "\n")
		for _,line := range lines{
			intersectionLen := 0
			RefSet := make(map[string]bool)
			strs := strings.FieldsFunc(line, Split)
			for _,str := range strs{ //construct the reference set/map
				RefSet[str] = true
			}
			//intersection of test set and reference set
			for k := range TestStructureMap{
				if RefSet[k] == true{
					intersectionLen++
				}
			}
			if intersectionLen>1000{
				score := float64(intersectionLen)/float64(len(TestStructureMap))
				sscore:=fmt.Sprintf("%.4f", score)
				fmt.Println("routine ID: "+strconv.Itoa(routineID) + ", insertersection length:" + sscore)
			}
	}

}
}
//Split is the function contains multi delimiters
func Split(r rune) bool {
    return r == ',' || r == '/'
}
