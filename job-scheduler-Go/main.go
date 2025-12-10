package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/robfig/cron/v3"
)

func runJob(i int) {
	fmt.Printf("Running job: %d\n", i+1)
}

func readFile(fileName string) string {
	content, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Println("Error reading file")
		return ""
	}

	data := string(content)
	return data;
}

func main() {

	content := readFile("/app/jobs.txt")

	lines := strings.Split(content, "\n")

	for  i, line := range lines {
		if line == ""{
			continue
		}
		c := cron.New()
		_, err := c.AddFunc(line, func() { runJob(i) })
		if err != nil {
			fmt.Println("Error adding cron job")
			return
		}
		c.Start()
	}

	select{}
}
