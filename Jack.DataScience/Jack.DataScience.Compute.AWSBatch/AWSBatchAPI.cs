using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.Batch;
using Amazon.Batch.Model;
using Amazon.Runtime;

namespace Jack.DataScience.Compute.AWSBatch
{
    public class AWSBatchAPI
    {
        private readonly AWSBatchOptions awsBatchOptions;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly AmazonBatchClient amazonBatchClient;
        private readonly SessionAWSCredentials sessionAWSCredentials;
        public AWSBatchAPI(AWSBatchOptions awsBatchOptions)
        {
            this.awsBatchOptions = awsBatchOptions;
            basicAWSCredentials = new BasicAWSCredentials(awsBatchOptions.Key, awsBatchOptions.Secret);
            amazonBatchClient = new AmazonBatchClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsBatchOptions.Region));
        }
        public AWSBatchAPI(SessionAWSCredentials sessionAWSCredentials)
        {
            this.sessionAWSCredentials = sessionAWSCredentials;
            var credentials = sessionAWSCredentials.GetCredentials();
            basicAWSCredentials = new BasicAWSCredentials(credentials.AccessKey, credentials.SecretKey);
            amazonBatchClient = new AmazonBatchClient(sessionAWSCredentials);
        }

        public AmazonBatchClient BatchClient { get => amazonBatchClient; }

        public async Task<bool> SubmitJob(string name, string jobARN, string queueARN, Dictionary<string, string> parameters)
        {
            await amazonBatchClient.SubmitJobAsync(new SubmitJobRequest()
            {
                JobName = name,
                JobDefinition = jobARN,
                JobQueue = queueARN,
                Parameters = parameters
            });
            return true;
        }

        public async Task<List<string>> ListJobs(string queueARN)
        {
            var response = await amazonBatchClient.ListJobsAsync(new ListJobsRequest()
            {
                JobQueue = queueARN
            });

            return response.JobSummaryList.Select(j => j.JobId).ToList();
        }

        public async Task<bool> CancelJobs(IEnumerable<string> jobIDs, string reason = "cancel")
        {
            foreach(var jobID in jobIDs)
            {
                await amazonBatchClient.CancelJobAsync(new CancelJobRequest()
                {
                    JobId = jobID,
                    Reason = reason
                });
            }
            return true;
        }

        public async Task<bool> CancelJob(string jobID, string reason = "cancel")
        {
            await amazonBatchClient.CancelJobAsync(new CancelJobRequest()
            {
                JobId = jobID,
                Reason = reason
            });
            return true;
        }

        public async Task<string> SubmitJobAndGetID(string name, string jobARN, string queueARN, Dictionary<string, string> parameters)
        {
            var response = await amazonBatchClient.SubmitJobAsync(new SubmitJobRequest()
            {
                JobName = name,
                JobDefinition = jobARN,
                JobQueue = queueARN,
                Parameters = parameters,
            });
            return response.JobId;
        }

        public async Task<JobDetail> IsJobCompleted(string jobID, string reason)
        {
            var response = await amazonBatchClient.DescribeJobsAsync(new DescribeJobsRequest()
            {
                Jobs = new List<string>() { jobID }
            });
            return response.Jobs[0];
        }
    }
}
