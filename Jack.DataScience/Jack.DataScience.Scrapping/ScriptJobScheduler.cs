using Amazon.DynamoDBv2.DocumentModel;
using Autofac;
using Jack.DataScience.Common;
using Jack.DataScience.Compute.AWSBatch;
using Jack.DataScience.Data.AWSDynamoDB;
using Jack.DataScience.MQ.AWSSQS;
using Jack.DataScience.Storage.AWSS3;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jack.DataScience.Scrapping
{
    public class ScriptJobScheduler
    {
        private readonly IComponentContext componentContext;
        private static string[] ScriptJobKeys = new string[] { nameof(ScriptJob.Script), nameof(ScriptJob.Job) };

        public ScriptJobScheduler(IComponentContext componentContext)
        {
            this.componentContext = componentContext;
        }

        private AWSS3API S3
        {
            get => componentContext.Resolve<AWSS3API>();
        }

        private AWSDynamoAPI DynamoDB
        {
            get => componentContext.Resolve<AWSDynamoAPI>();
        }

        private AWSBatchAPI Batch
        {
            get => componentContext.Resolve<AWSBatchAPI>();
        }

        private AWSSQSAPI SQS
        {
            get => componentContext.Resolve<AWSSQSAPI>();
        }

        private AWSScrapeJobOptions Options
        {
            get => componentContext.Resolve<AWSScrapeJobOptions>();
        }

        public async Task AddScriptJob(ScriptJob scriptJob)
        {
            var dynamoDB = DynamoDB;
            await dynamoDB.WriteItem(scriptJob);
        }

        /// <summary>
        /// this method should be run in Lambda, triggered by timer
        /// it checks all the scripts schedule them by adding them into SQS
        /// </summary>
        /// <param name="script"></param>
        /// <returns></returns>
        public async Task LoadJobQueue(IEnumerable<string> scripts)
        {
            var dynamoDB = DynamoDB;
            var sqs = SQS;
            var now = DateTime.UtcNow;
            foreach (var script in scripts)
            {
                var jobs = await dynamoDB.Query<ScriptJob>(nameof(ScriptJob.Script), QueryOperator.Equal, new List<object>() { script });
                jobs = jobs.Where(job => job.ShouldSchedule && job.LastSchedule.AddHours(job.TTL) < now).ToList();
                foreach (var job in jobs)
                {
                    // update the job and send to queue
                    await ScheduleJob(job, dynamoDB, sqs, now);
                }
            }
        }

        public async Task ForceLoadJobQueue(IEnumerable<string> scripts)
        {
            var dynamoDB = DynamoDB;
            var sqs = SQS;
            var now = DateTime.UtcNow;
            foreach (var script in scripts)
            {
                var jobs = await dynamoDB.Query<ScriptJob>(nameof(ScriptJob.Script), QueryOperator.Equal, new List<object>() { script });
                // schedule all jobs without conditions
                foreach (var job in jobs)
                {
                    // update the job and send to queue
                    await ScheduleJob(job, dynamoDB, sqs, now);
                }
            }
        }

        private static async Task ScheduleJob(ScriptJob job, AWSDynamoAPI dynamoDB, AWSSQSAPI sqs, DateTime now)
        {
            job.LastSchedule = now;
            job.State = ScriptJobStateEnum.Runnable;
            job.Attempts = 0;
            await dynamoDB.UpsertItem(job, ScriptJobKeys);
            await sqs.SendMessage(JsonConvert.SerializeObject(job, dynamoDB.JsonSerializerSettings));
        }

        /// <summary>
        /// this method polls ScrapeJob from SQS and schedule the BatchJob
        /// this method can be also calld by the Scrape docker task after its run
        /// </summary>
        /// <returns></returns>
        public async Task ScheduleScrapeJob(bool useRate = false)
        {
            var sqs = SQS;
            var batch = Batch;
            var options = Options;
            var count = await sqs.CountMessages();
            if (count > 0)
            {
                if (useRate && options.JobRate > 1)
                {
                    for(int i =0; i< count / options.JobRate; i++)
                    {
                        await batch.SubmitJob(options.JobName, options.JobARN, options.QueueARN, new Dictionary<string, string>());
                    }
                }
                else
                {
                    await batch.SubmitJob(options.JobName, options.JobARN, options.QueueARN, new Dictionary<string, string>());
                }
            }
        }

        /// <summary>
        /// get the ScriptJob from Queue and update the state as running and increase number of attempts
        /// the queue may contain duplicated jobs that were scheduled multiple times
        /// this method should also check if the job has completed and in TTL
        /// </summary>
        /// <returns></returns>
        public async Task<ScriptJobMessage> GetScriptJob()
        {
            var dynamoDB = DynamoDB;
            var sqs = SQS;
            var options = Options;

            if (options.TestMode)
            {
                if (options.TestQueue.Any())
                {
                    var jobFromQueue = options.TestQueue[0];
                    options.TestQueue.RemoveAt(0);
                    Console.WriteLine($"[TestMode] 1 ScriptJob retrieved from TestQueue");
                    return new ScriptJobMessage()
                    {
                        ReceiptHandle = "test-queue",
                        Job = jobFromQueue
                    };
                }
                else
                {
                    Console.WriteLine($"[TestMode] No ScriptJob found in TestQueue");
                    Debugger.Break();
                }
            }

            while(true)
            {
                var messages = await sqs.ReceiveMessage(1, options.InvisibilityTime);
                if (!messages.Any()) return null;
                var message = messages[0];
                var job = JsonConvert.DeserializeObject<ScriptJob>(message.Body, dynamoDB.JsonSerializerSettings);
                if(await dynamoDB.Exists(job, ScriptJobKeys))
                {
                    job.Attempts += 1;
                    job.State = ScriptJobStateEnum.Running;
                    // update the attempts
                    await dynamoDB.UpsertItem(job, ScriptJobKeys);
                    return new ScriptJobMessage()
                    {
                        ReceiptHandle = message.ReceiptHandle,
                        Job = job
                    };
                }
                else // not exists in the dynamoDB
                {
                    // delete the message and try next
                    await sqs.DeleteMessage(message.ReceiptHandle);
                }
            }  
        }

        /// <summary>
        /// this method should be called by the ScrapeEngine
        /// it validate the ScriptJob
        /// </summary>
        /// <param name="scriptJob"></param>
        /// <returns></returns>
        public async Task CreateScriptJob(ScriptJob scriptJob)
        {
            var dynamoDB = DynamoDB;
            var sqs = SQS;
            var options = Options;

            // in test mode, add to test queue and quite
            if (options.TestMode)
            {
                options.TestQueue.Add(scriptJob);
                return;
            }

            var now = DateTime.UtcNow;

            var found = await dynamoDB.ReadItem<ScriptJob>(
                    new Dictionary<string, object>()
                    {
                        { nameof(ScriptJob.Script), scriptJob.Script },
                        { nameof(ScriptJob.Job), scriptJob.Job }
                    }
                );

            if(found == null)
            {
                // this is a new job
                if (scriptJob.TTL == 0) scriptJob.TTL = options.DefaultTTL; // default is weekly 
                await ScheduleJob(scriptJob, dynamoDB, sqs, now);
            }
            else
            {
                // this is an existing job
                if(found.LastSchedule.AddHours(found.TTL) < now)
                {
                    // payload needs to be updated (very important!!!)
                    found.Payload = scriptJob.Payload;
                    // should schedule new
                    await ScheduleJob(found, dynamoDB, sqs, now);
                }
                // else it is ignored because it is still alive
            }
        }

        public async Task CompleteScriptJob(ScriptJobMessage message, bool success)
        {
            var dynamoDB = DynamoDB;
            var sqs = SQS;
            var options = Options;
            var job = message.Job;

            if (options.TestMode)
            {
                if (!success)
                {
                    if (job.Attempts < options.MaxAttempts)
                    {
                        job.State = ScriptJobStateEnum.Runnable;
                        options.TestQueue.Add(job);
                    }
                    else
                    {
                        job.State = ScriptJobStateEnum.Failed;
                        Console.WriteLine($"Job Failed in Test Mode: {job.Script}({job.Job})");
                    }
                }
                return;
            }
            // remove from the queue
            await sqs.DeleteMessage(message.ReceiptHandle);

            if (success)
            {
                job.State = ScriptJobStateEnum.Succeeded;
                await dynamoDB.UpsertItem(job, ScriptJobKeys);
            }
            else
            {
                if (job.Attempts >= options.MaxAttempts) // failed
                {
                    job.State = ScriptJobStateEnum.Failed;
                    // upsert the dynamo and do nothing
                    await dynamoDB.UpsertItem(job, ScriptJobKeys);
                }
                else
                {
                    job.State = ScriptJobStateEnum.Runnable;
                    // upsert the dynamo
                    await dynamoDB.UpsertItem(job, ScriptJobKeys);
                    // create new queue job
                    await sqs.SendMessage(JsonConvert.SerializeObject(job, dynamoDB.JsonSerializerSettings));
                }
            }
        }

    }
}
