package dp

import (
	"bytes"
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"io/ioutil"
	"testing"
)

func readAuth(filename string) string {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	return string(bytes.TrimSpace(buf))
}

func TestBasic(t *testing.T) {

	dp := New(aws.Auth{
		AccessKey: readAuth("/opt/cloudvm/keys/tim.key"),
		SecretKey: readAuth("/opt/cloudvm/keys/tim.secret.key"),
	}, aws.USWest2)

	req := &PipelineReq{
		PipelineIds: []string{
			"df-006272110MY5COX85G4J",
			"df-0282163WX59UA4DCX7H",
			"df-006272110MY5COX85G4J",
		},
	}
	resp, err := dp.DescribePipelines(req)
	fmt.Println("response is: ", resp, err)

}
