package main

import (
	"database/sql"
	"encoding/json"
	_ "errors"
	"fmt"
	"io/ioutil"
	"log"
	_ "math/rand"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	uuid "github.com/satori/go.uuid"
)

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func getUUID() string {
	//time.Now().Format("20060102_150405")
	//time.Now().Format("20060102") + fmt.Sprintf("_%d", time.Now().UnixNano())

	//rand.Seed(time.Now().UnixNano())
	//time.Now().Format("20060102") + fmt.Sprintf("_%d", rand.Intn(100000))
	return time.Now().Format("20060102") + fmt.Sprintf("_%s", uuid.Must(uuid.NewV4()))
}

func getCurrentPath() string {
	file, err := exec.LookPath(os.Args[0])
	if err != nil {
		return ""
	}
	path, err := filepath.Abs(file)
	if err != nil {
		return ""
	}
	i := strings.LastIndex(path, "/")
	if i < 0 {
		i = strings.LastIndex(path, "\\")
	}
	if i < 0 {
		return ""
	}
	return string(path[0:i])
}

func getConnection(host string, port int, user string, password string, dbname string) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", user, password, "tcp", host, port, dbname)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("Open mysql failed, error: %v\n", err)
		return nil, err
	} else {
		err = db.Ping()
		if err != nil {
			log.Printf("Connect mysql failed, error: %v\n", err)
			return nil, err
		}

		return db, nil
	}
}

func importSQL(file_path string, tb_name string, db_name string, line_type string) Message {
	if db_name == "" {
		db_name = dbConfig.Database
	}

	db, err := getConnection(dbConfig.Host, dbConfig.Port, dbConfig.User, dbConfig.Password, db_name)
	defer db.Close()
	if err != nil {
		return Message{Status: 0, Msg: err.Error()}
	}

	if _, err := os.Stat(file_path); err != nil {
		return Message{Status: 0, Msg: err.Error()}
	}
	file_path = strings.Replace(file_path, "\\", "/", -1)

	sql := fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '%s'", file_path, tb_name, getEnterString(line_type))
	//fmt.Println(sql)

	result, err := db.Exec(sql)
	if err != nil {
		log.Printf("Exec sql failed, error: %v\n", err)
		return Message{Status: 0, Msg: err.Error()}
	}
	aff_nums, err := result.RowsAffected()
	if err != nil {
		log.Printf("RowsAffected failed, error: %v\n", err)
		return Message{Status: 0, Msg: err.Error()}
	}

	return Message{Status: 1, Success: aff_nums}
}

func exportSQL(file_path string, sql string, db_name string, line_type string) Message {
	//delete tmp file
	if _, err := os.Stat(file_path); err == nil {
		if err := os.Remove(file_path); err != nil {
			log.Printf("Delete file failed, error: [%s]\n", err.Error())
		}
	}

	if db_name == "" {
		db_name = dbConfig.Database
	}

	db, err := getConnection(dbConfig.Host, dbConfig.Port, dbConfig.User, dbConfig.Password, db_name)
	defer db.Close()
	if err != nil {
		return Message{Status: 0, Msg: err.Error()}
	}

	file_path = strings.Replace(file_path, "\\", "/", -1)

	new_sql := fmt.Sprintf("%s INTO OUTFILE '%s' FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '%s'", sql, file_path, getEnterString(line_type))
	//fmt.Println(sql)

	result, err := db.Exec(new_sql)
	if err != nil {
		log.Printf("Exec sql failed, error: %v\n", err)
		return Message{Status: 0, Msg: err.Error()}
	}
	aff_nums, err := result.RowsAffected()
	if err != nil {
		log.Printf("RowsAffected failed, error: %v\n", err)
		return Message{Status: 0, Msg: err.Error()}
	}

	//read file content
	data := ""
	if _, err := os.Stat(file_path); err == nil {
		f, err := os.Open(file_path)
		if err != nil {
			log.Printf("Open file failed, error: %v\n", err)
			return Message{Status: 0, Msg: err.Error()}
		}

		d, err := ioutil.ReadAll(f)
		if err != nil {
			log.Printf("Read file failed, error: %v\n", err)
			return Message{Status: 0, Msg: err.Error()}
		}

		data = fmt.Sprintf("%d\n%s", aff_nums, string(d))
	}

	return Message{Status: 1, Data: data}
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(400)

	fmt.Fprintf(w, "Access denied.")
	return
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	//fmt.Println(r.Form)

	tb_name := strings.Join(r.Form["tb_name"], "")
	db_name := strings.Join(r.Form["db_name"], "")
	line_type := strings.Join(r.Form["line_type"], "")

	if tb_name == "" {
		w.WriteHeader(500)

		fmt.Fprintf(w, "Table name should not be empty.")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize)
	if err := r.ParseMultipartForm(maxUploadSize); err != nil {
		w.WriteHeader(500)

		fmt.Fprintf(w, "ParseMultipartForm failed, error: %s\n", err.Error())
		return
	}

	if len(r.MultipartForm.File) <= 0 {
		w.WriteHeader(500)

		fmt.Fprintf(w, "File should not be empty.")
		return
	}

	//fmt.Printf("File=>%v\n", r.MultipartForm.File)

	file_key := ""
	for k, _ := range r.MultipartForm.File {
		file_key = k
		break
	}

	file, handler, err := r.FormFile(file_key)
	defer file.Close()
	if err != nil {
		http.Error(w, "FormFile failed, error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	fileBytes, err := ioutil.ReadAll(file)
	if err != nil {
		http.Error(w, "ReadAll file failed, error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	//file path
	exist, _ := PathExists(uploadPath)
	if !exist {
		err := os.Mkdir(uploadPath, os.ModePerm)
		if err != nil {
			http.Error(w, "Create upload path failed, error: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	ext := path.Ext(handler.Filename)
	newPath := uploadPath + "/" + getUUID() + ext
	//fmt.Println(newPath)

	newFile, err := os.Create(newPath)
	defer newFile.Close()
	if err != nil {
		http.Error(w, "Create file failed, error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err := newFile.Write(fileBytes); err != nil {
		http.Error(w, "Write file failed, error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	newFile.Close()

	log.Printf("save file[%s] ok.\n", newPath)

	ret := importSQL(newPath, tb_name, db_name, line_type)
	//delete tmp file
	if _, err := os.Stat(newPath); err == nil {
		//fmt.Printf("Delete file[%s] ...\n", newPath)
		if err := os.Remove(newPath); err != nil {
			log.Printf("Delete file failed, error: [%s]\n", err.Error())
		}
	}

	if ret.Status <= 0 {
		http.Error(w, "importSQL failed, error: "+ret.Msg, http.StatusInternalServerError)
		return
	} else {
		json, err := json.Marshal(ret)
		if err != nil {
			http.Error(w, "Parse json failed, error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(json))
	}
}

func downloadHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	//fmt.Println(r.Form)

	db_name := strings.Join(r.Form["db_name"], "")
	line_type := strings.Join(r.Form["line_type"], "")

	raw_post, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "ReadAll failed, error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	sql := strings.TrimSpace(string(raw_post))
	if sql == "" {
		http.Error(w, "SQL should not be empty.", http.StatusInternalServerError)
		return
	}

	newPath := downloadPath + "/" + getUUID() + ".sql"

	ret := exportSQL(newPath, sql, db_name, line_type)

	//delete tmp file
	if _, err := os.Stat(newPath); err == nil {
		//fmt.Printf("Delete file[%s] ...\n", newPath)
		if err := os.Remove(newPath); err != nil {
			log.Printf("Delete file failed, error: [%s]\n", err.Error())
		}
	}

	if ret.Status <= 0 {
		http.Error(w, "importSQL failed, error: "+ret.Msg, http.StatusInternalServerError)
		return
	} else {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, ret.Data)
	}
}

func httpServer() {
	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/download", downloadHandler)

	log.Printf("HTTP Server running on %s:%d...\n", httpHost, httpPort)
	err := http.ListenAndServe(httpHost+":"+strconv.Itoa(httpPort), nil)
	if err != nil {
		log.Fatal(err)
	}
}

func getEnterString(line_type string) string {
	if line_type == "windows" {
		return "\\r\\n"
	} else {
		return "\\n"
	}
}

var httpHost string = "0.0.0.0"
var httpPort int = 3307
var uploadPath string = getCurrentPath() + "/tmp"
var downloadPath string = getCurrentPath() + "/tmp"

const maxUploadSize = 30 * 1024 * 2014 // 20 MB

type Message struct {
	Status  int    `json:"status"`
	Success int64  `json:"success"`
	Error   int64  `json:"error"`
	Msg     string `json:"msg"`
	Data    string `json:"data"`
}
type DBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

var dbConfig DBConfig

func main() {
	if len(os.Args) < 5 {
		log.Println("Command params should be: {port} {user} {password} {dbname}")
		return
	}
	if len(os.Args) >= 6 && os.Args[5] != "" {
		uploadPath = os.Args[5]
		downloadPath = os.Args[5]
	}

	port, _ := strconv.Atoi(os.Args[1])
	dbConfig = DBConfig{Host: "127.0.0.1", Port: port, User: os.Args[2], Password: os.Args[3], Database: os.Args[4]}

	db, err := getConnection(dbConfig.Host, dbConfig.Port, dbConfig.User, dbConfig.Password, dbConfig.Database)
	db.Close()

	/*
		d := importSQL("F:/golang/src/mysql-import/upload.sql", "var_setting2", dbConfig.Database, "windows")
		s, _ := json.Marshal(d)
		fmt.Println(string(s))
		return
	*/

	/*
		d := exportSQL("F:/golang/src/mysql-import/download.sql", "select * from spider_company limit 10", dbConfig.Database, "windows")
		fmt.Println(d)
		return
	*/

	if err != nil {
		log.Fatal(err)
	} else {
		httpServer()
	}
}
