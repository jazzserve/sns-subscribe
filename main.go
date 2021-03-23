package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/spf13/cobra"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
)

const (
	subConfirmType = "SubscriptionConfirmation"

	region   = "region"
	topic    = "topic"
	endpoint = "endpoint"
	port     = "port"
)

var (
	rootCmd = &cobra.Command{
		Use: "sns-subscribe",
	}

	subscribeCmd = &cobra.Command{
		Use: "subscribe",
		RunE: func(cmd *cobra.Command, args []string) error {
			port, _ := strconv.Atoi(cmd.Flag(port).Value.String())
			return subscribe(
				cmd.Flag(region).Value.String(),
				cmd.Flag(topic).Value.String(),
				cmd.Flag(endpoint).Value.String(),
				port,
			)
		},
	}
)

func init() {
	rootCmd.PersistentFlags().StringP(region, "r", "", "AWS region (required)")
	rootCmd.MarkPersistentFlagRequired(region)

	rootCmd.PersistentFlags().StringP(topic, "t", "", "AWS topic arn (required)")
	rootCmd.MarkPersistentFlagRequired(topic)

	rootCmd.PersistentFlags().StringP(endpoint, "e", "", "Endpoint (required)")
	rootCmd.MarkPersistentFlagRequired(endpoint)

	subscribeCmd.PersistentFlags().IntP(port, "p", 80, "Port (required)")

	rootCmd.AddCommand(subscribeCmd)
}

type handler struct {
	wg *sync.WaitGroup
}

func (s handler) confirmSubscription(subscribeURL string) {
	_, err := http.Get(subscribeURL)
	if err != nil {
		panic(err)
	} else {
		s.wg.Done()
	}
}

func (s handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	var f interface{}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(body, &f)
	if err != nil {
		panic(err)
	}

	data := f.(map[string]interface{})

	if data["Type"].(string) == subConfirmType {
		s.confirmSubscription(data["SubscribeURL"].(string))
	}
}

func subscribe(region string, topic string, endpoint string, port int) error {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return err
	}

	svc := sns.New(sess)

	endpointUrl, err := url.Parse(endpoint)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		http.Handle(endpointUrl.Path, handler{wg})
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
			panic(err)
		}
	}()

	_, err = svc.Subscribe(&sns.SubscribeInput{
		Endpoint: aws.String(endpoint),
		Protocol: aws.String(endpointUrl.Scheme),
		TopicArn: aws.String(topic),
	})
	if err != nil {
		return err
	}

	wg.Wait()

	fmt.Println("Successfully subscribed")

	return nil
}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
}
