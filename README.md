# Event-Driven-Microservices-with-Observability-Demo

## Design

`job-ingester` is a server-less consumer service validating and enriching job requests posted to the event bridge with a source of `jobs`. Those events are queued to the `ingester` queue which triggers the lambda to process it. `job-ingester` then queues the job on `jobs-todo`. Lambda fits the requirement here if the job receipt rate is sporadic or fairly low volume. That would not be the case if the worker processing rate were high volume. I also don't have EKS available in localstack community, nor Beanstalk for that matter, so its not possible to deploy the service as a DaemonSet. Another simple option would be to deploy the service as an EC2 ASG scaling the ASG based on the size of the queue.

`job-processor` reads jobs from the `jobs-todo` queue. It is deployed as a lambda as well and the same reasoning above applies.

### Message Processing

Jobs are fed as messages to AWS Event Bridge. Event Bridge is ideal for this as it can be used to filter incoming messages to respective queue keeping the interface simple for users if we extended support for different user requests.

The `ingester` SQS queue is fed from the event bridge if events have a source value of jobs. The ingester queue triggers the ingester lambda for each event. Again there are options here if the system were higher volume to have the lambda pull multiple events and process more for a single invocation. 

`ingester` lambda function validates the message as a valid job and if validation passes it posts the job on the `jobs-todo` SQS queue. `processor` lambda is trigger per event on the `jobs-todo` queue. The message is parsed again here (costing a small amount of compute but also allowing jobs to be fed to processing not via the ingester if we did want option). Finally the job is executed.

An SNS topic is notified if ingester or processor fail at any stage of their processing. 

Job messages which fail at any stage in ingest or processing are posted to a dead letter queue which is also the dead letter queue for both ingester and jobs-todo queue in case the lambda can't be triggered.

## Observability

Traces are written to jaeger. Metrics are automatically generated from the trace spans and sent to prometheus.
Open Telemetry Collector provides the glue for passing on the traces, exporting the metrics, and generating metrics from spans. I am using the contrib Open Telemetry image to get support for the spanmetrics connector.

## Starting The Demo

* clone the repo locally
* install [docker](https://docs.docker.com/engine/install/) and [docker-compose](https://docs.docker.com/compose/install/)
* Ensure the `GOOS` and `GOARCH` values in [.env](/.env) reflect your laptop build. It defaults to mac.
* `docker-compose up -d`
* Wait for the terraform_demo container to complete `docker-compose ps | grep terraform_demo | wc -l` should return 0. If 1 it's still running. Its takes a few minutes to build the resources needed in localstack.
* Run the event generator `cd go/job-generator/;./job-generator` which will run indefinitely generating random jobs, some malformed, and sleeping for a random interval between the bursts of jobs. If you only want the generator to run for a specific number of minutes use the `--minutes` flag.
* Examine your traces [here](http://localhost:16686/search)
* Examine your metrics [here](http://localhost:9090/query)
* Tear down with `docker-compose down`

## TO-DO

* Add prometheus/grafana deployment and a chart reflecting latency and success rate for jobs
* Make the logs available in a logging system such as ELK
* Subscribers to the SNS topic. Extend the schema to accept a notification target.
* The job schema propogation through the messages isn't as clean as I would like so could do with revisiting.