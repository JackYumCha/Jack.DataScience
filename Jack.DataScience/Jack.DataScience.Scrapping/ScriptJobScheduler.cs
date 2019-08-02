using Amazon.DynamoDBv2.DocumentModel;
using Autofac;
using Jack.DataScience.Cloud.HeartBeat;
using Jack.DataScience.Common;
using Jack.DataScience.Compute.AWSBatch;
using Jack.DataScience.Compute.AWSEC2;
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

        private AWSEC2API EC2
        {
            get => componentContext.Resolve<AWSEC2API>();
        }

        private AWSSQSAPI SQS
        {
            get => componentContext.Resolve<AWSSQSAPI>();
        }

        private AWSScrapeJobOptions Options
        {
            get => componentContext.Resolve<AWSScrapeJobOptions>();
        }

        private HeartBeatAPI HeartBeat
        {
            get => componentContext.Resolve<HeartBeatAPI>();
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
                Console.WriteLine($"Listing Jobs for {script}:");
                var jobs = await dynamoDB.Query<ScriptJob>(nameof(ScriptJob.Script), QueryOperator.Equal, new List<object>() { script });
                Console.WriteLine($"{jobs.Count} Jobs Found.");
                jobs = jobs.Where(job => job.ShouldSchedule && job.LastSchedule.AddHours(job.TTL) < now).ToList();
                Console.WriteLine($"{jobs.Count} Jobs Should be Scheduled.");
                foreach (var job in jobs)
                {
                    // update the job and send to queue
                    await ScheduleJob(job, dynamoDB, sqs, now);
                }
            }
        }

        public async Task ClearDynamoDBScript(IEnumerable<string> scripts)
        {
            var dynamoDB = DynamoDB;
            var sqs = SQS;
            var now = DateTime.UtcNow;
            foreach (var script in scripts)
            {
                Console.WriteLine($"Listing Jobs for {script}:");
                var jobs = await dynamoDB.Query<ScriptJob>(nameof(ScriptJob.Script), QueryOperator.Equal, new List<object>() { script });
                Console.WriteLine($"{jobs.Count} Jobs Will Be Deleted.");
                foreach (var job in jobs)
                {
                    // update the job and send to queue
                    await dynamoDB.DeleteItem(new Dictionary<string, object>() {
                        { nameof(ScriptJob.Script), script },
                        { nameof(ScriptJob.Job), job.Job }
                    });
                }
            }
        }

        public async Task LoadOneJobQueue(string script, string job)
        {
            var dynamoDB = DynamoDB;
            var sqs = SQS;
            var now = DateTime.UtcNow;

            var scriptJob = await dynamoDB.ReadItem<ScriptJob>(
                new Dictionary<string, object>()
                {
                                { nameof(ScriptJob.Script), script },
                                { nameof(ScriptJob.Job), job }
                });

            if(scriptJob != null)
            {
                await ScheduleJob(scriptJob, dynamoDB, sqs, now);
                Console.WriteLine($"ScriptJob Scheduled in Queue: [Script = {script}, Job = {job}] .");
            }
            else
            {
                Console.WriteLine($"ScriptJob not Found: [Script = {script}, Job = {job}] .");
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

        public async Task ForceLoadFailedSucceededJobQueue(IEnumerable<string> scripts)
        {
            var dynamoDB = DynamoDB;
            var sqs = SQS;
            var now = DateTime.UtcNow;
            foreach (var script in scripts)
            {
                var jobs = await dynamoDB.Query<ScriptJob>(nameof(ScriptJob.Script), QueryOperator.Equal, new List<object>() { script });
                jobs = jobs.Where(job => job.State == ScriptJobStateEnum.Failed || job.State == ScriptJobStateEnum.Succeeded).ToList();
                // schedule all jobs without conditions
                foreach (var job in jobs)
                {
                    // update the job and send to queue
                    await ScheduleJob(job, dynamoDB, sqs, now);
                }
            }
        }

        public async Task<List<ScriptJob>> GetFailedJobs(string script)
        {
            var dynamoDB = DynamoDB;
            var jobs = await dynamoDB.Query<ScriptJob>(nameof(ScriptJob.Script), QueryOperator.Equal, new List<object>() { script });
            return jobs.Where(job => job.State == ScriptJobStateEnum.Failed).ToList();
        }

        public async Task<List<ScriptJob>> GetRetryingJobs(string script)
        {
            var dynamoDB = DynamoDB;
            var jobs = await dynamoDB.Query<ScriptJob>(nameof(ScriptJob.Script), QueryOperator.Equal, new List<object>() { script });
            return jobs.Where(job => job.State == ScriptJobStateEnum.Runnable && job.Attempts > 0).ToList();
        }

        public async Task<List<ScriptJob>> GetAllJobs(string script)
        {
            var dynamoDB = DynamoDB;
            var jobs = await dynamoDB.Query<ScriptJob>(nameof(ScriptJob.Script), QueryOperator.Equal, new List<object>() { script });
            return jobs;
        }

        public async Task<ScriptJob> GetScriptJob(string script, string job)
        {
            var dynamoDB = DynamoDB;
            var item = await dynamoDB.ReadItem<ScriptJob>(new Dictionary<string, object>()
                {
                    {nameof(ScriptJob.Script), script},
                    {nameof(ScriptJob.Job),  job}
                });
            return item;
        }

        public async Task PurgeQueue()
        {
            await SQS.Purge();
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
            Console.WriteLine($"{count} Messages Found in Queue.");
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

        public async Task ScheduleEC2Job(bool useRate = false)
        {
            var sqs = SQS;
            var ec2 = EC2;
            var options = Options;
            var count = await sqs.CountMessages();
            Console.WriteLine($"{count} Messages Found in Queue.");

            var reservations = await ec2.DescribeAllInstances();

            var t2Smalls = reservations.SelectMany(r => r.Instances)
                .Where(i => i.KeyName == options.KeyPairName && i.Tags.Any(t => t.Key == "Name")
                        && i.Tags.FirstOrDefault(t => t.Key == "Name").Value == options.NameTag
                    )
                .ToList();

            var running = t2Smalls.Count(i => i.State.Name == "running");
            Console.WriteLine($"{running} EC2 Instances Running Currently.");

            if (useRate)
            {
                int take = (ec2.AWSEC2Options.StartIds.Count / options.JobRate) + 1;
                take = Math.Min(80, take);

                int instancesInNeed = take - running;

                if(instancesInNeed > 0)
                {
                    if(t2Smalls.Any(i => i.State.Name == "stopped"))
                    {
                        var instancesIdsToStart = t2Smalls.Where(i => i.State.Code == 80)
                            .Take(instancesInNeed)
                            .Select(i => i.InstanceId).ToList();

                        Console.WriteLine($"Starting {instancesIdsToStart.Count} EC2 Instances: {string.Join(", ", instancesIdsToStart)}");

                        await ec2.StartByIds(instancesIdsToStart);

                        instancesInNeed -= instancesIdsToStart.Count;
                    }
                }

                if(instancesInNeed > 0)
                {
                    var response = await ec2.RunInstanceByTemplate("lt-0c4c46a73c22ac44c", instancesInNeed);
                }
            }
            else
            {
                var instanceIds = t2Smalls
                    .Where(i => i.State.Name == "stopped")
                    .Select(i => i.InstanceId).ToList();

                Console.WriteLine($"Starting {instanceIds.Count} EC2 Instances: {string.Join(", ", instanceIds)}");
                await ec2.StartByIds(instanceIds);
            }
        }


        public async Task ScheduleMoreEC2Job(int number)
        {
            var sqs = SQS;
            var ec2 = EC2;
            var options = Options;
            var count = await sqs.CountMessages();
            Console.WriteLine($"{count} Messages Found in Queue.");

            var reservations = await ec2.DescribeAllInstances();

            var instances = reservations.SelectMany(r => r.Instances)
                .Where(i => i.KeyName == options.KeyPairName && i.Tags.Any(t => t.Key == "Name")
                        && i.Tags.FirstOrDefault(t => t.Key == "Name").Value == options.NameTag
                    )
                .ToList();

            var running = instances.Count(i => i.State.Code == 16);
            var stopped = instances.Count(i => i.State.Code == 80);
            Console.WriteLine($"{running} EC2 Instances Running Currently.");
            Console.WriteLine($"{stopped} EC2 Instances Running Currently.");

            if(stopped > 0)
            {
                Console.WriteLine($"Stopped Instances will be Started before Launching More.");
                var instanceIDsToStart = instances.Where(i => i.State.Code == 80).Select(i => i.InstanceId).ToList();
                await ec2.StartByIds(instanceIDsToStart);
            }
            else
            {
                Console.WriteLine($"Launch {number} More Instances.");
                var response = await ec2.RunInstanceByTemplate(options.LaunchTemplateId, number);
            }
            
        }

        public async Task CheckEC2Health()
        {
            var sqs = SQS;
            var ec2 = EC2;
            var options = Options;
            var heartBeat = HeartBeat;
            var dynamo = heartBeat.DynamoDB;

            var count = await sqs.CountMessages();
            Console.WriteLine($"{count} Messages Found in Queue.");

            var reservations = await ec2.DescribeAllInstances();

            var instances = reservations.SelectMany(r => r.Instances)
                .Where(i => i.KeyName == options.KeyPairName && i.Tags.Any(t => t.Key == "Name")
                        && i.Tags.FirstOrDefault(t => t.Key == "Name").Value == options.NameTag
                    )
                .ToList();

            var found = await dynamo.Scan<HeartBeatSignal>(nameof(HeartBeatSignal.Job), QueryOperator.Equal, new List<object>() { "ScrapeEngine" });

            Console.WriteLine($"Time: {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")} Instances: {found.Count}");
            var now = DateTime.UtcNow;
            found.GroupBy(s => s.Log)
                .ToList()
                .ForEach(g =>
                {
                    var instanceCount = g.Count().ToString().PadLeft(2);
                    var total = g.Sum(s => s.Count).ToString().PadLeft(2);
                    var max = g.Max(s => s.Count).ToString().PadLeft(4);
                    var maxRun = g.Max(s => s.MaxRun).ToString().PadLeft(4).ToString().PadLeft(2);
                    var c10 = g.Count(s => 0d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes && (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes <= 10d).ToString().PadLeft(2);
                    var c20 = g.Count(s => 10d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes && (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes <= 20d).ToString().PadLeft(2);
                    var c30 = g.Count(s => 20d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes && (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes <= 30d).ToString().PadLeft(2);
                    var c40 = g.Count(s => 30d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes && (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes <= 40d).ToString().PadLeft(2);
                    var c50 = g.Count(s => 40d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes && (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes <= 50d).ToString().PadLeft(2);
                    var c60 = g.Count(s => 50d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes && (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes <= 60d).ToString().PadLeft(2);
                    var hourPlus = g.Count(s => 60d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes).ToString().PadLeft(2);
                    Console.WriteLine($"Instance Type: {g.Key.PadLeft(9)}: Count: {instanceCount}, Total: {total}, Max: {max}, MaxRun: {maxRun}, 0-10min: {c10}, 10-20min: {c20}, 20-30min: {c30}, 30-40min: {c40}, 40-50min: {c50}, 50-60min: {c60}, 1hour+: {hourPlus}");
                });

            await heartBeat.CheckHealthOf(options.HeartBeatJob, instances.Select(s => s.InstanceId), new OperationTypeEnum[] { OperationTypeEnum.Reboot }, OperationTypeEnum.TerminateAndLaunchNew);

        }

        public async Task RecycleEC2Instances()
        {
            var sqs = SQS;
            var ec2 = EC2;
            var options = Options;
            var count = await sqs.CountMessages();
            Console.WriteLine($"{count} Messages Found in Queue.");

            var reservations = await ec2.DescribeAllInstances();

            var instances = reservations.SelectMany(r => r.Instances)
                .Where(i => i.KeyName == options.KeyPairName && i.Tags.Any(t => t.Key == "Name")
                        && i.Tags.FirstOrDefault(t => t.Key == "Name").Value == options.NameTag
                    )
                .ToList();
            var running = instances.Count(i => i.State.Code == 16);
            var stopped = instances.Count(i => i.State.Code == 80);
            Console.WriteLine($"{running} EC2 Instances Running Currently. {stopped} EC2 Instances Stopped.");

            if (count == 0)
            {
                if(stopped > 0)
                {
                    var instanceIdsToTerminate = instances.Where(i => i.State.Code == 80).Select(i => i.InstanceId).ToList();
                    Console.WriteLine($"Terminate {instanceIdsToTerminate.Count} Stopped Instances: {string.Join(", ", instanceIdsToTerminate)}");
                    await ec2.TerminateByIds(instanceIdsToTerminate);
                }
            }
            else
            {
                Console.WriteLine($"Recycle will run when SQS is cleared.");
            }
        }

        public async Task RebootEC2Job(bool useRate = false)
        {
            var sqs = SQS;
            var ec2 = EC2;
            var options = Options;
            var count = await sqs.CountMessages();
            Console.WriteLine($"{count} Messages Found in Queue.");

            var reservations = await ec2.DescribeAllInstances();

            var instances = reservations.SelectMany(r => r.Instances)
                .Where(i => i.KeyName == options.KeyPairName && i.Tags.Any(t => t.Key == "Name")
                        && i.Tags.FirstOrDefault(t => t.Key == "Name").Value == options.NameTag
                    )
                .ToList();

            var running = instances.Count(i => i.State.Code == 16);
            Console.WriteLine($"{running} EC2 Instances Running Currently.");

            var instanceIds = instances.Where(i => i.State.Code == 16).Select(i => i.InstanceId).ToList();

            Console.WriteLine($"Rebooting {instanceIds.Count} EC2 Instances: {string.Join(", ", instanceIds)}");
            await ec2.RebootByIds(instanceIds);
        }

        public async Task StopEC2Job(bool useRate = false)
        {
            var ec2 = EC2;
            var options = Options;
            var reservations = await ec2.DescribeAllInstances();

            var t2Smalls = reservations.SelectMany(r => r.Instances)
                .Where(i => i.KeyName == options.KeyPairName && i.Tags.Any(t => t.Key == "Name")
                        && i.Tags.FirstOrDefault(t => t.Key == "Name").Value == options.NameTag
                    )
                .ToList();

            var running = t2Smalls.Count(i => i.State.Name == "running");
            Console.WriteLine($"{running} EC2 Instances Running Currently.");

            var instanceIds = t2Smalls.Where(i => i.State.Name == "running").Select(i => i.InstanceId).ToList();

            Console.WriteLine($"Stopping EC2 Instances: {string.Join(", ", instanceIds)}");

            await ec2.StopByIds(instanceIds);
        }

        public async Task TerminateEC2Job(bool useRate = false)
        {
            var ec2 = EC2;
            var options = Options;
            var reservations = await ec2.DescribeAllInstances();

            var t2Small = reservations.SelectMany(r => r.Instances)
                .Where(i => i.KeyName == options.KeyPairName && i.Tags.Any(t => t.Key == "Name")
                        && i.Tags.FirstOrDefault(t => t.Key == "Name").Value == options.NameTag
                    )
                .ToList();

            var instanceIds = t2Small.Select(i => i.InstanceId).ToList();

            Console.WriteLine($"Stopping EC2 Instances: {string.Join(", ", instanceIds)}");

            await ec2.TerminateByIds(instanceIds);
        }

        public async Task CancelAllJob()
        {
            var batch = Batch;
            var options = Options;
            var jobs = await batch.ListJobs(options.QueueARN);
            await batch.CancelJobs(jobs);
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
                var found = await dynamoDB.ReadItem<ScriptJob>(new Dictionary<string, object>()
                {
                    {nameof(ScriptJob.Script), job.Script},
                    {nameof(ScriptJob.Job),  job.Job}
                });
                if (found != null)
                {
                    if (found.State == ScriptJobStateEnum.Succeeded && found.LastSchedule.AddHours(found.TTL) > DateTime.UtcNow)
                    {
                        Common.Logging.Console.WriteLine($"Drop Duplicated Message ${found.Script}->{found.Job}");
                        await sqs.DeleteMessage(message.ReceiptHandle);
                    }
                    else
                    {
                        found.Attempts += 1;
                        found.State = ScriptJobStateEnum.Running;
                        found.Payload = job.Payload;
                        // update the attempts
                        await dynamoDB.UpsertItem(found, ScriptJobKeys);
                        return new ScriptJobMessage()
                        {
                            ReceiptHandle = message.ReceiptHandle,
                            Job = job
                        };
                    }
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
                // payload needs to be updated (very important!!!)
                found.Payload = scriptJob.Payload;
                found.TTL = scriptJob.TTL;
                await dynamoDB.UpsertItem(found, ScriptJobKeys);
                // this is an existing job
                if (found.LastSchedule.AddHours(found.TTL) < now)
                {
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
            if(!string.IsNullOrWhiteSpace(message.ReceiptHandle))
                await sqs.DeleteMessage(message.ReceiptHandle);

            if (success)
            {
                job.State = ScriptJobStateEnum.Succeeded;
                await dynamoDB.UpsertItem(job, ScriptJobKeys);
                if (string.IsNullOrWhiteSpace(message.ReceiptHandle))
                {
                    Common.Logging.Console.WriteLine($"Update DynamoDB with Success {job.Script}->{job.Job}");
                }
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
