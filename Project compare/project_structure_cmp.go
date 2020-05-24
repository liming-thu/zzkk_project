package main
import (
 "fmt"
 "strconv"
 "database/sql"
 "strings"
 "time"
 _"github.com/go-sql-driver/mysql"
)

//RN is the number of routines
var RN = 128

var fileCh = make(chan string,200)
var routineCh = make(chan bool, RN)

var files []string

//TestSet is the set to be searched in the files
var TestSet = make(map[string]bool) 

var db *sql.DB

func main(){
	initDB()
	var testStructure string
	db.QueryRow("select Structure from test_project_property").Scan(&testStructure)
	
	for _,str := range strings.FieldsFunc(testStructure,Split){
		TestSet[str] = true
	}

	tp,_ := db.Query("select Structure from t_project_property")	
	var ts []string
	for tp.Next(){
		var p string
		tp.Scan(&p)
		ts=append(ts,p)
	}

	startTime := time.Now()

	for i := 1; i<RN; i++ {
		go cmp(i)
	}

	for i:=0; i<len(ts); i++{
		fileCh <- ts[i]
	}
	close(fileCh)
	for i := 1; i<RN; i++ {
		<-routineCh
	}

	fmt.Println("TIME USED (ms): "+ strconv.FormatInt(time.Since(startTime).Milliseconds(),10))
}

func initDB() (err error){
	host := "root:liming@tcp(localhost:3306)/zzkk"
	db,err =sql.Open("mysql",host)
	if err!=nil{
		print(err.Error())
		return err
	}
	err = db.Ping()
	if err!=nil{
		return err
	}
	return nil
}

func cmp(routineID int){	
	for {
		t, ok := <-fileCh
		if !ok{
			routineCh<-true
			break;
		}
		lines := strings.Split(string(t), "\n")
		for _,line := range lines{
			intersectionLen := 0
			RefSet := make(map[string]bool)
			strs := strings.FieldsFunc(line, Split)
			for _,str := range strs{ //construct the reference set/map
				RefSet[str] = true
			}
			//intersection of test set and reference set
			for k := range TestSet{
				if RefSet[k] == true{
					intersectionLen++
				}
			}
			if intersectionLen>1000{
				score := float64(intersectionLen)/float64(len(TestSet))
				sscore:=fmt.Sprintf("%.3f", score)
				fmt.Println("routine ID: "+strconv.Itoa(routineID) + ", insertersection length:" + sscore)
			}
	}

}
}
//Split is the function contains multi delimiters
func Split(r rune) bool {
    return r == ',' || r == '_'
}