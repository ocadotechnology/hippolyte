# Hippolyte
Hippolyte an at-scale, point-in-time backup solution for DynamoDB. It is designed to handle frequent, recurring backups of large numbers of tables, scale read throughput, and batch together backup jobs over multiple EMR clusters.

## Deployment
Hippolyte is deployed with the [Serverless Framework](https://serverless.com/). This can be installed with `npm`. To start with run:
```
npm install serverless
npm install --save serverless-python-requirements
```
 To configure the project for your Amazon accounts, update `hippolyte/project_config.py` with the details of the account in which you intend to run the backup process, you will also need AWS credentials for creating all the dependent resources:
* Lambda Function
* CloudWatch scheduled events
* SNS topic

To deploy the stack run
`serverless deploy --region <aws_region> --stage <stage> --email <email_address>` 
You can update `serverless.yml` to associate your credentials with stages if you intended to deploy multiple instances of the service. The email setting is optional and uses SNS to alert the provided address to an failed pipelines or tables.

## Motivation
Since DynamoDB is a fully managed service and supports cross-region replication you may wonder why you even need to backup data in the first place. If you're running production applications on AWS then you probably already have a lot of confidence in the durability of data in services like DynamoDB or S3.

Our motivation for building this was to protect against application or user error. No matter how durable Amazon's services are, they wont protect you from unintended updates and deletes. Historical snapshots of data and state also provide additional value, allowing you to restore to a separate table and compare against live data.

## Design
We've chosen [Amazon Data Pipeline](https://aws.amazon.com/datapipeline/) as a tool to create, manage and run our backup tasks. Data Pipeline helps with orchestration, automatic retries for failed jobs and the potential to make use of SNS notifications for successful or failed EMR tasks.

We also use AWS Lambda to schedule and monitor backup jobs. This is responsible for dynamically generating Data Pipeline templates based on configuration and discovered tables, and modifying table throughputs to reduce the duration of the backup job.

## Scaling
Part of the job of our scheduling Lambda function is to attempt to optimally assign DynamoDB tables to individual EMR clusters that will be created. Since new tables may be created each day and size may grow significantly, this optimisation is performed each night during the scheduling step. By default, each data pipeline only supports 100 objects; this means each pipeline can support 32 tables, this is because each tables requires 3 Data Pipeline objects:

* DynamoDBDataNode
* EmrActivity
* S3DataNode

In addition to this 2 additional objects are needed, the pipeline configuration and an EmrCluster node. 32 * 3 + 2 = 98. In addition to this hard limit we also want every backup to run between 12:00 AM and 7:00 AM. We can work out how long each pipeline will take to complete by starting with some static values

* EMR cluster bootstrap (10 min)
* EMR activity bootstrap (1 min)

From there calculating how long each table will take to backup, this can be done with the following formula:

$$$
Duration = \frac {Size}{RCU * ConsumedPercentage * 4096\ bytes/second}
$$$

Where _Size_ is the table size in bytes, _RCU_ is the provisioned Read Capacity Units for a given table and _ConsumedPercentage_ is what proportion of this capacity the backup job will use. Since each EMR cluster will run backup jobs sequentially and we have limits to the number of tables and length of time, we can pack each pipeline with tables until one of those two constraints is met.

Additionally some tables are either too large to be backed up in a timely manner with their provisioned read capacity. Here we derive the ratio between the expected backup duration and what is desired and increase our read capacity units by this ratio. We can also increase the percentage of provisioned throughput we consume while preserving the original amount needed for the application. Typically since we paying for clusters and capacity by the hour, it's rarely worth reduce the total expected duration to be less than that.
