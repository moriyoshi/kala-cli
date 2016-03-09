package main

import (
	"fmt"
	kala_c "github.com/ajvb/kala/client"
	kala_job "github.com/ajvb/kala/job"
	"github.com/codegangsta/cli"
	"os"
	"strings"
)

func formatBool(v bool, falseStr, trueStr string) string {
	if v {
		return trueStr
	} else {
		return falseStr
	}
}

func putSeparator() {
	fmt.Println("---")
}

func formatJob(j *kala_job.Job) {
	fmt.Printf("Job id: %s\n", j.Id)
	fmt.Printf("Name: %s\n", j.Name)
	fmt.Printf("Disabled: %s\n", formatBool(j.Disabled, "no", "yes"))
	fmt.Printf("Schedule: %s\n", j.Schedule)
	fmt.Printf("Owner: %s\n", j.Owner)
	fmt.Printf("Command: %s\n", j.Command)
	fmt.Printf("Retries: %d\n", j.Retries)
	fmt.Printf("Parent jobs: %s\n", strings.Join(j.ParentJobs, ", "))
	fmt.Printf("Dependent jobs: %s\n", strings.Join(j.DependentJobs, ", "))
	fmt.Printf("Success count: %d\n", j.Metadata.SuccessCount)
	fmt.Printf("Error count: %d\n", j.Metadata.ErrorCount)
	fmt.Printf("Last success: %s\n", j.Metadata.LastSuccess)
	fmt.Printf("Last error: %s\n", j.Metadata.LastError)
	fmt.Printf("Last attempted run: %s\n", j.Metadata.LastAttemptedRun)
}

func formatJobStat(n int, js *kala_job.JobStat) {
	fmt.Printf("Repetition: %d\n", n)
	fmt.Printf("Last run at: %s\n", js.RanAt)
	fmt.Printf("Number of retries: %d\n", js.NumberOfRetries)
	fmt.Printf("Status: %s\n", formatBool(js.Success, "failed", "success"))
	fmt.Printf("Execution duration: %s\n", js.ExecutionDuration)
}

func formatKalaStats(ks *kala_job.KalaStats) {
	fmt.Printf("Stats retrieved at: %s\n", ks.CreatedAt)
	fmt.Printf("Total jobs: %d\n", ks.Jobs)
	fmt.Printf("Active jobs: %d\n", ks.ActiveJobs)
	fmt.Printf("Disabled jobs: %d\n", ks.DisabledJobs)
	fmt.Printf("Success count: %d\n", ks.SuccessCount)
	fmt.Printf("Error count: %d\n", ks.ErrorCount)
	fmt.Printf("Next run: %s\n", ks.NextRunAt)
	fmt.Printf("Last attempted run: %s\n", ks.LastAttemptedRun)
}

func main() {
	app := cli.NewApp()
	app.Name = "Kala CLI"
	app.Version = "Command-Line Interface for Kala Job Scheduler"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "endpoint, E",
			Value: "http://localhost:8000",
			Usage: "Endpoint where Kala API is running",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:    "stats",
			Aliases: []string{"stat"},
			Usage:   "Display scheduler's statistics information",
			Flags:   []cli.Flag{},
			Action: func(ctx *cli.Context) {
				c := kala_c.New(ctx.GlobalString("endpoint"))
				ks, err := c.GetKalaStats()
				if err != nil {
					fmt.Fprintf(ctx.App.Writer, "%s: %v\n", ctx.App.HelpName, err)
					os.Exit(1)
				}
				formatKalaStats(ks)
			},
		},
		{
			Name:  "create-job",
			Usage: "Create a job",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "owner",
					Usage:  "Email address of the job's owner",
					EnvVar: "USER",
				},
				cli.IntFlag{
					Name:  "retries",
					Value: 0,
					Usage: "Number of times to retry on failed attempt for each run",
				},
				cli.StringFlag{
					Name:  "epsilon",
					Value: "",
					Usage: "Duration in which it is safe to retry the job",
				},
			},
			Action: func(ctx *cli.Context) {
				args := ctx.Args()
				if len(args) < 3 {
					fmt.Fprintf(ctx.App.Writer, "%s: The first argument is a job name, the second is a schedule spec and the third is a command line to run.\n", ctx.App.HelpName)
					os.Exit(255)
				}
				retries := ctx.Int("retries")
				if retries < 0 {
					fmt.Fprintf(ctx.App.Writer, "%s: You cannot specify a negative value for --retries.\n", ctx.App.HelpName)
					os.Exit(255)
				}
				c := kala_c.New(ctx.GlobalString("endpoint"))
				id, err := c.CreateJob(&kala_job.Job{
					Owner:    ctx.String("owner"),
					Retries:  uint(retries),
					Name:     args.Get(0),
					Schedule: args.Get(1),
					Command:  args.Get(2),
				})
				if err != nil {
					fmt.Fprintf(ctx.App.Writer, "%s: %v\n", ctx.App.HelpName, err)
					os.Exit(1)
				}
				fmt.Println(id)
			},
		},
		{
			Name:  "delete-job",
			Usage: "Delete a job",
			Flags: []cli.Flag{},
			Action: func(ctx *cli.Context) {
				args := ctx.Args()
				if len(args) < 1 {
					fmt.Fprintf(ctx.App.Writer, "%s: The first argument is a job id.\n", ctx.App.HelpName)
					os.Exit(255)
				}
				c := kala_c.New(ctx.GlobalString("endpoint"))
				ok, err := c.DeleteJob(args.Get(0))
				if err != nil {
					fmt.Fprintf(ctx.App.Writer, "%s: %v\n", ctx.App.HelpName, err)
					os.Exit(1)
				}
				fmt.Printf("%s\n", formatBool(ok, "failed", "success"))
				if !ok {
					os.Exit(2)
				}
			},
		},
		{
			Name:  "list-jobs",
			Usage: "List all jobs",
			Flags: []cli.Flag{},
			Action: func(ctx *cli.Context) {
				c := kala_c.New(ctx.GlobalString("endpoint"))
				jobs, err := c.GetAllJobs()
				if err != nil {
					fmt.Fprintf(ctx.App.Writer, "%s: %v\n", ctx.App.HelpName, err)
					os.Exit(1)
				}
				for _, j := range jobs {
					fmt.Println(j.Id)
				}
			},
		},
		{
			Name:  "describe-job",
			Usage: "Describe a job",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "stats",
					Usage: "Display statistics information",
				},
			},
			Action: func(ctx *cli.Context) {
				args := ctx.Args()
				if len(args) < 1 {
					fmt.Fprintf(ctx.App.Writer, "%s: The first argument is a job id.\n", ctx.App.HelpName)
					os.Exit(255)
				}
				c := kala_c.New(ctx.GlobalString("endpoint"))
				j, err := c.GetJob(args.Get(0))
				if err != nil {
					fmt.Fprintf(ctx.App.Writer, "%s: %v\n", ctx.App.HelpName, err)
					os.Exit(1)
				}
				sts := ([]*kala_job.JobStat)(nil)
				if ctx.Bool("stats") {
					sts, err = c.GetJobStats(args.Get(0))
					if err != nil {
						fmt.Fprintf(ctx.App.Writer, "%s: %v\n", ctx.App.HelpName, err)
						os.Exit(1)
					}
				}
				formatJob(j)
				if sts != nil {
					for i, st := range sts {
						putSeparator()
						formatJobStat(i, st)
					}
				}
			},
		},
	}
	app.Run(os.Args)
}
