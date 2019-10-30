package logshipper

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type AzureLogShipper struct {
	WorkspaceID string
	SharedKey   string
	HttpProxy   string
}

func (a *AzureLogShipper) SendLog(logType string, length int, body *bytes.Reader) (err error) {
	azureUrl := fmt.Sprintf("https://%v.ods.opinsights.azure.com/api/logs", a.WorkspaceID)
	req, err := http.NewRequest("POST", azureUrl, body)
	if err != nil {
		return err
	}

	dateString := strings.Replace(time.Now().UTC().Format(time.RFC1123), "UTC", "GMT", 1)

	authHeader, err := a.createAuthHeader(length, dateString)
	if err != nil {
		return err
	}

	req.Header.Add("Log-Type", logType)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("x-ms-date", dateString)
	req.Header.Add("Authorization", authHeader)

	q := req.URL.Query()
	q.Add("api-version", "2016-04-01")
	req.URL.RawQuery = q.Encode()

	var client *http.Client

	if a.HttpProxy != "" {
		proxyURL, err := url.Parse("http://localhost:62269")
		if err != nil {
			return err
		}

		client = &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	} else {
		client = http.DefaultClient
	}

	res, err := client.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		return fmt.Errorf("Error sending log file, got status code %v", res.StatusCode)
	}

	return nil
}

func (a *AzureLogShipper) createAuthHeader(length int, dateString string) (string, error) {
	stringToHash := fmt.Sprintf("POST\n%v\napplication/json\nx-ms-date:%v\n/api/logs", length, dateString)
	encodedKey, err := base64.StdEncoding.DecodeString(a.SharedKey)
	if err != nil {
		return "", err
	}

	h := hmac.New(sha256.New, encodedKey)
	h.Write([]byte(stringToHash))
	hash := h.Sum(nil)
	sha := base64.StdEncoding.EncodeToString(hash)

	authHeader := fmt.Sprintf("SharedKey %v:%v", a.WorkspaceID, sha)

	return authHeader, nil
}
