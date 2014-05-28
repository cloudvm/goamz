// This package provides types and functions to interact Data Pipeline service
package dp

import (
	//"fmt"
	"github.com/crowdmob/goamz/aws"
	"net/http"
	//"net/http/httputil"
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"time"
	//  "github.com/bitly/go-simplejson"
)

type DP struct {
	aws.Auth
	aws.Region
}

const (
	DataPipelineEndpoint = "https://datapipeline.us-west-2.amazonaws.com/"
)

func New(auth aws.Auth, region aws.Region) *DP {
	return &DP{auth, region}
}

type PipelineReq struct {
	PipelineIds []string `json:"pipelineIds"`
}

type Field struct {
	Key         string `json:"key"`
	RefValue    string `json:"refValue,omitempty"`
	StringValue string `json:"stringValue,omitempty"`
}

type CreatePipelineReq struct {
	Description string `json:"description,omitempty"`
	Name        string `json:"name"`
	UniqueId    string `json:"uniqueId"`
}

type CreatePipelineResp struct {
	PipelineId string `json:"pipelineId"`
}

type DeletePipelineReq struct {
	PipelineId string `json:"pipelineId"`
}

type ActivatePipelineReq struct {
	PipelineId string `json:"pipelineId"`
}

type DescribeObjectsReq struct {
	EvaluateExpressions bool     `json:"evaluateExpressions,omitempty"`
	Marker              string   `json:"marker,omitempty"`
	ObjectIds           []string `json:"objectIds"`
	PipelineId          string   `json:"pipelineId"`
}

type PipelineObject struct {
	Fields []Field `json:"fields"`
	Id     string  `json:"id,omitempty"`
	Name   string  `json:"name,omitempty"`
}

type DescribeObjectsResp struct {
	HasMoreResults  bool
	Marker          string
	PipelineObjects []PipelineObject
}

type DescribePipelinesResp struct {
	PipelineDescriptionList []PipelineDescription
}

type PipelineDescription struct {
	Description string
	Fields      []Field
	Name        string
	PipelineId  string
}

type ExpressionReq struct {
	Expression string `json:"expression"`
	ObjectId   string `json:"objectId"`
	PipelineId string `json:"pipelineId"`
}

type ExpressionResp struct {
	EvaluatedExpression string
}

type GetPipelineDefinitionReq struct {
	PipelineId string `json:"pipelineId"`
	Version    string `json:"version,omitempty"`
}

type GetPipelineDefinitionResp struct {
	PipelineObjects []PipelineObject
}

type ListPipelinesReq struct {
	Marker string `json:"marker,omitempty"`
}

type PipelineIds struct {
	Id   string
	Name string
}

type ListPipelinesResp struct {
	HasMoreResults bool
	Marker         string
	PipelineIdList []PipelineIds
}

type PipelineDefinitionReq struct {
	PipelineId      string           `json:"pipelineId"`
	PipelineObjects []PipelineObject `json:"pipelineObjects"`
}

type ValidationError struct {
	Errors []string
	Id     string
}

type ValidationWarning struct {
	Id       string
	Warnings []string
}

type PipelineDefinitionResp struct {
	Errored            bool
	ValidationErrors   []ValidationError
	ValidationWarnings []ValidationWarning
}

type PollForTaskReq struct {
	Hostname         string `json:"hostname,omitempty"`
	InstanceIdentity struct {
		Document  string `json:"document,omitempty"`
		Signature string `json:"signature,omitempty"`
	} `json:"instanceIdentity,omitempty"`
	WorkerGroup string `json:"workerGroup"`
}

type Object struct {
	String PipelineObject `json:"string"`
}

type PollForTaskResp struct {
	TaskObject struct {
		AttemptId  string
		Objects    []Object
		PipelineId string
		TaskId     string
	}
}

type Selector struct {
	FieldName string
	Operator  struct {
		Type   string
		Values []string
	}
}

type QueryObjectsReq struct {
	Limit      int    `json:"limit,omitempty"`
	Marker     string `json:"marker,omitempty"`
	PipelineId string `json:"pipelineId"`
	Query      struct {
		Selectors []Selector `json:"selectors,omitempty"`
	} `json:"query,omitempty"`
	Sphere string `json:"sphere"`
}

type QueryObjectsResp struct {
	HasMoreResults bool
	Ids            []string
	Marker         string
}

type ReportTaskProgressReq struct {
	TaskId string `json:"taskId"`
}

type ReportTaskProgressResp struct {
	Canceled bool
}

type ReportTaskRunnerHeartbeatReq struct {
	Hostname     string `json:"hostname,omitempty"`
	TaskrunnerId string `json:"taskrunnerId"`
	WorkerGroup  string `json:"workerGroup,omitempty"`
}

type ReportTaskRunnerHeartbeatResp struct {
	Terminate bool
}

type SetStatusReq struct {
	ObjectIds  []string `json:"objectIds"`
	PipelineId string   `json:"pipelineId"`
	Status     string   `json:"status"`
}

type SetTaskStatusReq struct {
	ErrorId         string `json:"errorId,omitempty"`
	ErrorMessage    string `json:"errorMessage,omitempty"`
	ErrorStackTrace string `json:"erroStackTrace,omitempty"`
	TaskId          string `json:"taskId"`
	TaskStatus      string `json:"taskStatus"`
}

func (dp *DP) queryServer(action string, postData []byte) (int, []byte, error) {
	hreq, err := http.NewRequest("POST", DataPipelineEndpoint, bytes.NewReader(postData))
	if err != nil {
		return 0, nil, err
	}
	hreq.Header.Set("Content-Type", "application/x-amz-json-1.1")
	hreq.Header.Set("X-Amz-Date", time.Now().UTC().Format(aws.ISO8601BasicFormat))
	hreq.Header.Set("X-Amz-Target", "DataPipeline."+action)
	signer := aws.NewV4Signer(dp.Auth, "datapipeline", dp.Region)
	signer.Sign(hreq)
	// dump, err := httputil.DumpRequestOut(hreq, false)
	// if err == nil {
	//   fmt.Println("Dump: ", string(dump))
	// }
	resp, err := http.DefaultClient.Do(hreq)
	if err != nil {
		return 0, nil, err
	}

	defer resp.Body.Close()
	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Could not read response body")
		return 0, nil, err
	}
	if resp.StatusCode != 200 {
		return resp.StatusCode, nil, buildError(resp, respBuf)
	}
	return resp.StatusCode, respBuf, err
}

func (dp *DP) ActivatePipeline(req *ActivatePipelineReq) error {
	buf, err := json.Marshal(req)
	if err != nil {
		return err
	}
	_, _, err = dp.queryServer("ActivatePipeline", buf)
	if err != nil {
		return err
	}
	return nil
}

func (dp *DP) CreatePipeline(req *CreatePipelineReq) (*CreatePipelineResp, error) {
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, body, err := dp.queryServer("CreatePipeline", buf)
	if err != nil {
		return nil, err
	}
	var jsonResp CreatePipelineResp
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		return nil, err
	}
	return &jsonResp, nil
}

func (dp *DP) DeletePipeline(req *DeletePipelineReq) error {
	buf, err := json.Marshal(req)
	if err != nil {
		return err
	}
	_, _, err = dp.queryServer("DeletePipeline", buf)
	if err != nil {
		return err
	}
	return nil
}

func (dp *DP) DescribeObjects(req *DescribeObjectsReq) (*DescribeObjectsResp, error) {
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, body, err := dp.queryServer("DescribeObjects", buf)
	if err != nil {
		return nil, err
	}
	var jsonResp DescribeObjectsResp
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		return nil, err
	}
	return &jsonResp, nil
}

func (dp *DP) DescribePipelines(req *PipelineReq) (*DescribePipelinesResp, error) {
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, body, err := dp.queryServer("DescribePipelines", buf)
	if err != nil {
		return nil, err
	}
	var jsonResp DescribePipelinesResp
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		return nil, err
	}
	return &jsonResp, nil
}

func (dp *DP) EvaluateExpression(req *ExpressionReq) (*ExpressionResp, error) {
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, body, err := dp.queryServer("EvaluateExpression", buf)
	if err != nil {
		return nil, err
	}
	var jsonResp ExpressionResp
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		return nil, err
	}
	return &jsonResp, nil
}

func (dp *DP) GetPipelineDefinition(req *GetPipelineDefinitionReq) (*GetPipelineDefinitionResp, error) {
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, body, err := dp.queryServer("GetPipelineDefinition", buf)
	if err != nil {
		return nil, err
	}
	var jsonResp GetPipelineDefinitionResp
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		return nil, err
	}
	return &jsonResp, nil
}

func (dp *DP) ListPipelines(req *ListPipelinesReq) (*ListPipelinesResp, error) {
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, body, err := dp.queryServer("ListPipelines", buf)
	if err != nil {
		return nil, err
	}
	var jsonResp ListPipelinesResp
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		return nil, err
	}
	return &jsonResp, nil
}

func (dp *DP) PollForTask(req *PollForTaskReq) (*PollForTaskResp, error) {
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, body, err := dp.queryServer("PollForTask", buf)
	if err != nil {
		return nil, err
	}
	var jsonResp PollForTaskResp
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		return nil, err
	}
	return &jsonResp, nil
}

func (dp *DP) PutPipelineDefinition(req *PipelineDefinitionReq) (*PipelineDefinitionResp, error) {
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, body, err := dp.queryServer("PutPipelineDefinition", buf)
	if err != nil {
		return nil, err
	}
	var jsonResp PipelineDefinitionResp
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		return nil, err
	}
	return &jsonResp, nil
}

func (dp *DP) QueryObjects(req *QueryObjectsReq) (*QueryObjectsResp, error) {
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, body, err := dp.queryServer("QueryObjects", buf)
	if err != nil {
		return nil, err
	}
	var jsonResp QueryObjectsResp
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		return nil, err
	}
	return &jsonResp, nil
}

func (dp *DP) ReportTaskProgress(req *ReportTaskProgressReq) (*ReportTaskProgressResp, error) {
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, body, err := dp.queryServer("ReportTaskProgress", buf)
	if err != nil {
		return nil, err
	}
	var jsonResp ReportTaskProgressResp
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		return nil, err
	}
	return &jsonResp, nil
}

func (dp *DP) ReportTaskRunnerHeartbeat(req *ReportTaskRunnerHeartbeatReq) (*ReportTaskRunnerHeartbeatResp, error) {
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, body, err := dp.queryServer("ReportTaskRunnerHeartbeat", buf)
	if err != nil {
		return nil, err
	}
	var jsonResp ReportTaskRunnerHeartbeatResp
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		return nil, err
	}
	return &jsonResp, nil
}

func (dp *DP) SetStatus(req *SetStatusReq) error {
	buf, err := json.Marshal(req)
	if err != nil {
		return err
	}
	_, _, err = dp.queryServer("SetStatus", buf)
	if err != nil {
		return err
	}
	return nil
}

func (dp *DP) SetTaskStatus(req *SetTaskStatusReq) error {
	buf, err := json.Marshal(req)
	if err != nil {
		return err
	}
	_, _, err = dp.queryServer("SetTaskStatus", buf)
	if err != nil {
		return err
	}
	return nil
}

func (dp *DP) ValidatePipelineDefinition(req *PipelineDefinitionReq) (*PipelineDefinitionResp, error) {
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, body, err := dp.queryServer("ValidatePipelineDefinition", buf)
	if err != nil {
		return nil, err
	}
	var jsonResp PipelineDefinitionResp
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		return nil, err
	}
	return &jsonResp, nil
}
