package main
import (
	//_"github.com/go-sql-driver/mysql"
	//"database/sql"
	//"os"
	"github.com/unidoc/unioffice/document"
)


//CreateWordReport is ...
func CreateWordReport(FileName string){
	doc := document.New()
	para := doc.AddParagraph()
	run := para.AddRun()
	run.AddText("这是由Go生成的文档")
	doc.AddHeader()
	doc.SaveToFile(FileName)
}