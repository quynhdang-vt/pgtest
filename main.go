package main

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	sqlxTypes "github.com/jmoiron/sqlx/types"
	_ "github.com/lib/pq"
	"log"
	"os"
	"reflect"
	"time"

	"io"
	"net/http"
	"strings"
)

type Task struct {
	TaskId            string             `db:"task_id"`
	JobId             string             `db:"job_id"`
	ApplicationId     string             `db:"application_id"`
	CreatedDateTime   int                `db:"created_date_time"`
	QueuedDateTime    int                `db:"queued_date_time"`
	ModifiedDateTime  int                `db:"modified_date_time"`
	CompletedDateTime int                `db:"completed_date_time"`
	TaskOrder         int                `db:"task_order"`
	TaskExecutor      string             `db:"task_executor"`
	TaskPayload       sqlxTypes.JSONText `db:"task_payload"`
	TaskOutput        sqlxTypes.JSONText `db:"task_output"`
	RecordingId       string             `db:"recording_id"`
	IsClone           bool               `db:"is_clone"`
	EngineId          string             `db:"engine_id"`
	FailureType       string             `db:"failure_type"`
	TaskLog           string             `db:"task_log"`
	SourceAssetId     string             `db:"source_asset_id"`
	EnginePrice       int                `db:"engine_price"`
	CustomerPrice     int                `db:"customer_price"`
	MediaLengthSecs   int                `db:"media_length_secs"`
	MediaStorageBytes int                `db:"media_storage_bytes"`
	MediaFileName     string             `db:"media_file_name"`
	BusinessUnit      string             `db:"busines_unit"`
	BuildId           string             `db:"build_id"`
	RateCardPrice     int                `db:"rate_card_price"`
	Payload           sqlxTypes.JSONText `db:"payload"`
	TestTask          bool               `db:"test_task`
	StartedDateTime   int                `db:"started_date_time"`
}

func main() {
	noProg := os.Args[1:]
	for _, v := range noProg {
		recordingId:=strings.TrimSuffix(v, ".txt")
		uri := getOldestMediaUri(recordingId)
		// got it!! Need to download it, retaining the.
		i := strings.LastIndex(uri, ".")
		var suffix string
		if i == -1 {
			suffix = ".tmp"
		} else {
			suffix = uri[i:]
		}
		filename := "/tmp/recording/" + recordingId + suffix
		err := DownloadFile(uri, filename)
		if err != nil {
			log.Fatalf("Failed to download file for %s, %v\n", recordingId, err)
		}
	}
}

type RecordingAsset struct {
	Uri             string `db:"uri"`
	CreatedDateTime int    `db:"created_date_time"`
}

/**
given a recordingId, query the platform/recording/recording_asset table something like this:
SELECT uri,created_date_time
FROM recording.recording_asset
WHERE recording_id = '41614394'
AND type='media'
ORDER by created_date_time ASC
*/
func getOldestMediaUri(recordingId string) string {
	var PG_PLATFORM = os.Getenv("PG_PROD_CONN")
        if len(PG_PLATFORM)==0 {
           log.Fatalf("Please definte the PG_PROD_CONN environment variable")
        }
	query := fmt.Sprintf("SELECT uri, created_date_time FROM recording.recording_asset WHERE recording_id='%s' AND type='media' ORDER by created_date_time ASC", recordingId)
	log.Printf("getting uri for %s\nQuery=%s\n", recordingId, query)
	db, err := sqlx.Connect("postgres", PG_PLATFORM)
	if err != nil {
		log.Fatalf("Failed to connect..%v\n", err)
	}
	recording := RecordingAsset{}
	rows, err := db.Queryx(query)
	if err != nil {
		log.Fatalf("Failed to query..%v\n", err)
	}
	for rows.Next() {
		err := rows.StructScan(&recording)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("%#v\n", recording)
		// returns the first one..
		return recording.Uri
	}
	return ""
}
func ChangeTask() {
	fmt.Println("Hello THERE")
	db, err := sqlx.Connect("postgres", "postgres://postgres:L0r3mtocsum4m0r3@localhost:15432/platform?sslmode=disable")
	if err != nil {
		log.Fatalln(err)
	}

	task := Task{}
	err = db.Get(&task, "SELECT task_id, job_id, task_output, task_payload FROM job_new.task WHERE recording_id='27'")
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("TaskId=%s, JobId=%s, TaskOutput=%s, TaskPayload=%s\n", task.TaskId, task.JobId, task.TaskOutput, task.TaskPayload)

	valueMap := map[string]interface{}{
		"task_status":         "completed",
		"completed_date_time": time.Now().Unix(),
	}
	err = UpdateTask(db, task.TaskId, valueMap)
	if err != nil {
		fmt.Printf(err.Error())
	}
}
func TraceValues(values []interface{}) {
	for _, value := range values {
		fmt.Printf("%v, ", value)
	}
}

func UpdateTask(DB *sqlx.DB, taskId string, valueMap map[string]interface{}) error {

	nChanges := len(valueMap)
	if nChanges == 0 {
		return nil
	}
	keys := reflect.ValueOf(valueMap).MapKeys()
	values := make([]interface{}, 1+nChanges, 1+nChanges)
	values[0] = taskId
	values[1] = valueMap[keys[0].String()]

	sqlStatement := fmt.Sprintf("UPDATE job_new.task SET %s = $2", keys[0].String())
	for i := 1; i < len(keys); i++ {
		sqlStatement = fmt.Sprintf("%s, %s = $%d", sqlStatement, keys[i], i+2)
		values[i+1] = valueMap[keys[i].String()]
	}
	sqlStatement = fmt.Sprintf("%s WHERE task_id = $1;", sqlStatement)

	fmt.Printf("SQL Statement='%s'\n", sqlStatement)
	fmt.Printf("Values:=")
	TraceValues(values)
	res, err := DB.Exec(sqlStatement, values...)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return err
	}
	count, err := res.RowsAffected()
	if err != nil {
		return err
	}
	fmt.Printf("\n%d rows affected.\n", count)
	return nil
}
func DownloadFile(url string, designated string) error {

	fmt.Printf("DownloadFile ENTER url=%s --> %s\n", url, designated)
	var _, err = os.Stat(designated)
	if err == nil {
		//already exists
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	out, err := os.Create(designated)
	if err != nil {
		return err
	}
	defer func() { out.Close(); fmt.Printf("DownloadFile EXIT - check %s\n", designated) }()

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}
