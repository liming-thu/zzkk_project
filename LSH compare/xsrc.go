package main 
import "fmt"
import "os"
import "path/filepath"
import "strconv"
//import "bufio"
import "io/ioutil"
import "strings"
import "time"

//RN is the number of routines
var RN = 128

var fileCh = make(chan string,200)
var routineCh = make(chan bool, RN)

var files []string

//TestSet is the set to be searched in the files
var TestSet = make(map[string]bool) 

func main(){
	
	rootPath := "/home/liming/lsh_data"

	filepath.Walk(rootPath, getFile)

	for _,str := range strings.Split("710008,eabfdcb26992fb0a7d20018412fc3397425195e4,HashCode31LT7,506fa825,506fa825,a8eb3a79,43b566e7,858e7110,858e7110,43a13815,caf814f0,bd94dce8,02079936,9f7394f6,9f7394f6,cfe07628,1a71561a,6b61ea6f,1c0d2277,297ea82c,5f54f972,6a8c235f,a4b3baf3,6e1af061,f16d0252,96a388da,36a13dc1,6e094786,5bd19dab,c7eab13b,bd503f33,c5c36434,8a1f3a2d,2a1d8f36,72b5f571,72b5f571,487279c8,a039ad49,3b77e0a7,9ff18a07,b48b31b8,5f23afed,5f23afed,5884cb06,cc83213e,fdd785f1,1fab2e7b,7c38907e,97900e2b,97900e2b,ada57aa3,96740bbc,23c3db39,4d2bce82,5c2cb38,5c2cb38,5c2cb38", ","){
		TestSet[str] = true
	}

	startTime := time.Now()

	for i := 1; i<RN; i++ {
		go cmp(i)
	}

	for i:=0; i<len(files); i++{
		fileCh <- files[i]
	}
	close(fileCh)
	for i := 1; i<RN; i++ {
		<-routineCh
	}

	fmt.Println("TIME USED (ms): "+ strconv.FormatInt(time.Since(startTime).Milliseconds(),10))
}

func getFile(path string ,info os.FileInfo, err error) error{
	if !info.IsDir() {
		files = append(files,path)
	}
	return nil
}

func cmp(routineID int){	
	for {
		path, ok := <-fileCh
		if !ok{
			routineCh<-true
			break;
		}
		file,err := os.Open(path)
		if err !=nil{
			fmt.Println("Fetal error, reading file: "+path +" "+err.Error())
		}

		// scanner :=bufio.NewScanner(file)
		// for scanner.Scan(){
		// 	intersectionLen := 0
		// 	RefSet := make(map[string]bool)
		// 	strs := strings.Split(scanner.Text(), ",")
		// 	for _,str := range strs{ //construct the reference set/map
		// 		RefSet[str] = true
		// 	}
		// 	//intersection of test set and reference set
		// 	for k := range TestSet{
		// 		if RefSet[k] == true{
		// 			intersectionLen++
		// 		}
		// 	}
		// 	if intersectionLen>10{
		// 		fmt.Println("routine ID: "+strconv.Itoa(routineID) +path + ", insertersection length:" + strconv.Itoa(intersectionLen))
		// 	}
		// }
		content,_ := ioutil.ReadFile(path)
		lines := strings.Split(string(content), "\n")
		for _,line := range lines{
			intersectionLen := 0
			RefSet := make(map[string]bool)
			strs := strings.Split(line, ",")
			for _,str := range strs{ //construct the reference set/map
				RefSet[str] = true
			}
			//intersection of test set and reference set
			for k := range TestSet{
				if RefSet[k] == true{
					intersectionLen++
				}
			}
			if intersectionLen>10{
				fmt.Println("routine ID: "+strconv.Itoa(routineID) +path + ", insertersection length:" + strconv.Itoa(intersectionLen))
			}
		}

		file.Close()
	}

}