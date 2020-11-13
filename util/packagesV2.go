package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"io/ioutil"
	"net/http"
	"time"
)

func packagesV2(name string, num int) {

	for {
		jobJson := sPop(packageV2Queue)
		if jobJson == "" {
			time.Sleep(1 * time.Second)
			continue
		}

		// Json decode
		JobMap := make(map[string]string)
		err := json.Unmarshal([]byte(jobJson), &JobMap)
		if err != nil {
			fmt.Println(getProcessName(name, num), "JSON Decode Error:", jobJson)
			sAdd(packageV2Set+"-json_decode_error", jobJson)
			continue
		}

		actionType, ok := JobMap["type"]
		if !ok {
			fmt.Println(getProcessName(name, num), "package field not found: type")
			continue
		}

		if actionType == "update" {
			updatePackageV2(JobMap, name, num,false)
		}

		if actionType == "delete" {
			deletePackageV2(JobMap, name, num)
		}

	}

}

func packagesV2Resty(name string, num int) {

	for {
		jobJson := sPop(packageV2QueueRetry)
		if jobJson == "" {
			time.Sleep(1 * time.Second)
			continue
		}

		// Json decode
		JobMap := make(map[string]string)
		err := json.Unmarshal([]byte(jobJson), &JobMap)
		if err != nil {
			fmt.Println(getProcessName(name, num), "JSON Decode Error:", jobJson)
			sAdd(packageV2Set+"-json_decode_error", jobJson)
			continue
		}

		actionType, ok := JobMap["type"]
		if !ok {
			fmt.Println(getProcessName(name, num), "package field not found: type")
			continue
		}
		time.Sleep(5*time.Second)
		if actionType == "update" {
			updatePackageV2(JobMap, name, num,true)
		}

		if actionType == "delete" {
			deletePackageV2(JobMap, name, num)
		}

	}

}

func updatePackageV2(JobMap map[string]string, name string, num int,isRetry bool) {
	packageName, ok := JobMap["package"]
	if !ok {
		fmt.Println(getProcessName(name, num), "package field not found: package")
		return
	}

	updateTime, ok := JobMap["time"]
	if !ok {
		fmt.Println(getProcessName(name, num), "package field not found: time")
		return
	}

	path := "p2/" + packageName + ".json"
	header := make(http.Header)
	lastSyncTime := hGet(packageV2SetUpdateTime, packageName)
	if lastSyncTime != "" {
		header.Set("If-Modified-Since", lastSyncTime)
	}
	resp, err := getJSONWithHeader(packagistUrl(path),header, getProcessName(name, num))

	if err != nil {
		fmt.Println(getProcessName(name, num), path, err.Error())
		makeFailed(packageV2Set, path, err)
		return
	}

	// 如果是304 且不是重试的情况下 就重新投递
	if resp.StatusCode == 304 && !isRetry{
		jsonV2, _ := json.Marshal(JobMap)
		sAdd(packageV2QueueRetry, string(jsonV2))
		return
	}

	if resp.StatusCode != 200 {
		makeStatusCodeFailed(packageV2Set, resp.StatusCode, path)
		return
	}

	content, err := ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		fmt.Println(getProcessName(name, num), path, err.Error())
		return
	}

	content, err = decode(content)
	if err != nil {
		fmt.Println("parseGzip Error", err.Error())
		return
	}

	// JSON Decode
	packageJson := make(map[string]interface{})
	err = json.Unmarshal(content, &packageJson)
	if err != nil {
		fmt.Println(getProcessName(name, num), "JSON Decode Error:", path)
		return
	}

	_, ok = packageJson["minified"]
	if !ok {
		fmt.Println(getProcessName(name, num), "package field not found: minified")
		return
	}

	// Put to OSS
	options := []oss.Option{
		oss.ContentType("application/json"),
	}
	err = putObject(getProcessName(name, num), path, bytes.NewReader(content), options...)
	if err != nil {
		syncHasError = true
		fmt.Println("putObject Error", err.Error())
		return
	}

	hSet(packageV2Set, packageName, updateTime)
	hSet(packageV2SetUpdateTime, packageName, resp.Header.Get("Last-Modified"))

	cdnCache(path, name, num)
}

func deletePackageV2(JobMap map[string]string, name string, num int) {
	packageName, ok := JobMap["package"]
	if !ok {
		fmt.Println(getProcessName(name, num), "package field not found: package")
		return
	}

	path := "p2/" + packageName + ".json"

	hDel(packageV2Set, packageName)
	deleteObject(getProcessName(name, num), path)
}
