using Amazon.DynamoDBv2.DocumentModel;
using Jack.DataScience.Compute.AWSEC2;
using Jack.DataScience.Data.AWSDynamoDB;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Jack.DataScience.Cloud.HeartBeat
{
    public class HeartBeatAPI
    {
        private readonly HeartBeatOptions heartBeatOptions;
        private readonly AWSEC2API ec2;
        private readonly AWSDynamoAPI dynamoDB;

        public HeartBeatAPI(HeartBeatOptions heartBeatOptions)
        {
            this.heartBeatOptions = heartBeatOptions;
            ec2 = new AWSEC2API(heartBeatOptions.EC2);
            dynamoDB = new AWSDynamoAPI(heartBeatOptions.DynamoDB);
        }
        
        public AWSDynamoAPI DynamoDB { get => dynamoDB; }

        public async Task HeartBeat(HeartBeatSignal signal)
        {
            var found = await dynamoDB.ReadItem<HeartBeatSignal>(new Dictionary<string, object>()
                {
                    { nameof(HeartBeatSignal.InstanceId), signal.InstanceId },
                    { nameof(HeartBeatSignal.Job), signal.Job }
                });
            if (found != null)
            {
                signal.Count = found.Count + 1;
                signal.LastRun =(int)Math.Ceiling((signal.LastSignalTimestamp.ToUniversalTime() - found.LastSignalTimestamp.ToUniversalTime()).TotalSeconds);
                signal.LastPayload = found.Payload;
                if(signal.LastRun > found.MaxRun)
                {
                    signal.MaxRun = signal.LastRun;
                    signal.MaxPayload = signal.LastPayload;
                }
                else
                {
                    signal.MaxRun = found.MaxRun;
                    signal.MaxPayload = found.MaxPayload;
                }
            }
            else
            {
                signal.Count = 1;
            }
            await dynamoDB.UpsertItem(signal, new string[] { nameof(HeartBeatSignal.InstanceId), nameof(HeartBeatSignal.Job) });
        }

        public async Task Reset(HeartBeatSignal signal)
        {
            signal.LastSignalTimestamp = DateTime.UtcNow;
            signal.Count = 0;
            await dynamoDB.UpsertItem(signal, new string[] { nameof(HeartBeatSignal.InstanceId), nameof(HeartBeatSignal.Job) });
        }

        public async Task CheckHealthOf(string job, IEnumerable<string> instanceIds, IEnumerable<OperationTypeEnum> checks, OperationTypeEnum defaultOperation)
        {
            foreach(var instanceId in instanceIds)
            {
                var signal = await dynamoDB.ReadItem<HeartBeatSignal>(new Dictionary<string, object>()
                {
                    { nameof(HeartBeatSignal.InstanceId), instanceId },
                    { nameof(HeartBeatSignal.Job), job }
                });
                if(signal == null)
                {
                    await ApplyOperation(instanceId, defaultOperation);
                }
                else
                {
                    await CheckInstance(signal, checks);
                }
            }
        }

        public async Task CheckHealth(string job, IEnumerable<OperationTypeEnum> checks)
        {
            var signals = await dynamoDB.Query<HeartBeatSignal>(nameof(HeartBeatSignal.Job), QueryOperator.Equal, new List<object>() { job });
            foreach(var signal in signals)
            {
                await CheckInstance(signal, checks);
            }
        }

        public async Task CheckInstance(HeartBeatSignal signal, IEnumerable<OperationTypeEnum> checks)
        {
            foreach (var check in checks)
            {
                switch (check)
                {
                    case OperationTypeEnum.Reboot:
                        if (signal.RebootTimeout > 0 && signal.LastSignalTimestamp.AddSeconds(signal.RebootTimeout) < DateTime.UtcNow)
                        {
                            await ApplyOperation(signal.InstanceId, OperationTypeEnum.Reboot);
                            await Reset(signal);
                            return;
                        }
                        break;
                    case OperationTypeEnum.Stop:
                        if (signal.StopTimeout > 0 && signal.LastSignalTimestamp.AddSeconds(signal.StopTimeout) < DateTime.UtcNow)
                        {
                            await ApplyOperation(signal.InstanceId, OperationTypeEnum.Stop);
                            await Reset(signal);
                            return;
                        }
                        break;
                    case OperationTypeEnum.Terminate:
                        if (signal.TerminateTimeout > 0 && signal.LastSignalTimestamp.AddSeconds(signal.TerminateTimeout) < DateTime.UtcNow)
                        {
                            await ApplyOperation(signal.InstanceId, OperationTypeEnum.Terminate);
                            await Reset(signal);
                            return;
                        }
                        break;
                    case OperationTypeEnum.LaunchMore:
                        if (signal.LaunchMoreTimeout > 0 && signal.LastSignalTimestamp.AddSeconds(signal.LaunchMoreTimeout) < DateTime.UtcNow)
                        {
                            await ApplyOperation(signal.InstanceId, OperationTypeEnum.LaunchMore, signal.LaunchTemplateId);
                            await Reset(signal);
                            return;
                        }
                        break;
                }
            }
        }

        public async Task ApplyOperation(string instanceId, OperationTypeEnum operation, string launchTemplateId = null)
        {
            switch (operation)
            {
                case OperationTypeEnum.Reboot:
                    await ec2.RebootByIds(new List<string>() { instanceId });
                    break;
                case OperationTypeEnum.Stop:
                    await ec2.StopByIds(new List<string>() { instanceId });
                    break;
                case OperationTypeEnum.Terminate:
                    await ec2.TerminateByIds(new List<string>() { instanceId });
                    break;
                case OperationTypeEnum.LaunchMore:
                    if (string.IsNullOrWhiteSpace(launchTemplateId))
                        await ec2.RunInstanceByTemplate(heartBeatOptions.DefaultLaunchTemplateId);
                    else
                        await ec2.RunInstanceByTemplate(launchTemplateId);
                    break;
                case OperationTypeEnum.TerminateAndLaunchNew:
                    await ec2.TerminateByIds(new List<string>() { instanceId });
                    if (string.IsNullOrWhiteSpace(launchTemplateId))
                        await ec2.RunInstanceByTemplate(heartBeatOptions.DefaultLaunchTemplateId);
                    else
                        await ec2.RunInstanceByTemplate(launchTemplateId);
                    break;
            }
        }

        public async Task GenerateStatistics()
        {
            //var heartBeat = services.Resolve<HeartBeatAPI>();
 
            var dynamo = DynamoDB;

            var found = await dynamo.Scan<HeartBeatSignal>(nameof(HeartBeatSignal.Job), QueryOperator.Equal, new List<object>() { "ScrapeEngine" });

            Console.WriteLine($"Time: {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")} Instances in Dynamo: {found.Count}");
            var now = DateTime.UtcNow;

            //Console.WriteLine($"ec2 is instance: {(ec2 != null)}");
            var reservations = await ec2.DescribeAllInstances();

            reservations
                .Where(instance => instance != null && instance.Instances != null).ToList();

            //Console.WriteLine($"Number of Reservations: {reservations.Count}");

            var instances = reservations
                .SelectMany(instance => instance.Instances)
                .Where(instance => instance.State != null && instance.Tags != null)
                .ToList();

            //Console.WriteLine($"Number of Instances: {instances.Count}");

            var engines = instances
                .Where(instance => instance != null && instance.State != null && instance.State.Code == 16 && instance.Tags != null && instance.Tags.Any(tag => tag != null && tag.Key == "Name" && !string.IsNullOrWhiteSpace(tag.Value) && tag.Value.ToLower().Contains("scrape-engine")))
                .ToList();

            Console.WriteLine($"Number of EC2 Engines: {engines.Count}");

            var engineIds = engines.Select(engine => engine.InstanceId).ToList();

            var enginesNotInFound = engineIds.Except(found.Select(record => record.InstanceId)).ToList();

            Console.WriteLine($"Instances Not In HeartBeat: { string.Join(", ", enginesNotInFound)}");
            var expired = found.Where(s => 90d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes).ToList();

            Console.WriteLine($"Instances Not Updating > 1.5 Hour: { string.Join(", ", expired.OrderBy(s => s.LastSignalTimestamp).Select(s => $"{s.InstanceId}({(now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes.ToString("0")}min)"))}");
            
 
            found.GroupBy(s => s.Log)
                .ToList()
                .ForEach(g =>
                {
                    var count = g.Count().ToString().PadLeft(2);
                    var total = g.Sum(s => s.Count).ToString().PadLeft(2);
                    var max = g.Max(s => s.Count).ToString().PadLeft(4);
                    var maxRun = g.Max(s => s.MaxRun).ToString().PadLeft(4).ToString().PadLeft(2);
                    var c10 = g.Count(s => 0d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes && (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes <= 10d).ToString().PadLeft(2);
                    var c20 = g.Count(s => 10d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes && (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes <= 20d).ToString().PadLeft(2);
                    var c30 = g.Count(s => 20d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes && (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes <= 30d).ToString().PadLeft(2);
                    var c40 = g.Count(s => 30d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes && (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes <= 40d).ToString().PadLeft(2);
                    var c50 = g.Count(s => 40d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes && (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes <= 50d).ToString().PadLeft(2);
                    var c60 = g.Count(s => 50d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes && (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes <= 60d).ToString().PadLeft(2);
                    var c70 = g.Count(s => 60d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes && (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes <= 70d).ToString().PadLeft(2);
                    var c80 = g.Count(s => 70d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes && (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes <= 80d).ToString().PadLeft(2);
                    var c90 = g.Count(s => 80d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes && (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes <= 90d).ToString().PadLeft(2);
                    
                    var cOT = g.Count(s => 90d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes);

                    Console.WriteLine($"Group {g.Key.PadLeft(9)}: Count: {count}, Total: {total}, Max: {max}, MaxRun: {maxRun}");
                    Console.WriteLine($"00-10min: {c10}, 10-20min: {c20}, 20-30min: {c30}, 30-40min: {c40}, 40-50min: {c50}, 50-60min: {c60}");
                    Console.WriteLine($"60-70min: {c70}, 70-80min: {c80}, 80-90min: {c90}, Over 90min: {cOT}");
                });
 
        }
        public async Task KillOver90Min()
        {
            //var heartBeat = services.Resolve<HeartBeatAPI>();

            var dynamo = DynamoDB;

            var found = await dynamo.Scan<HeartBeatSignal>(nameof(HeartBeatSignal.Job), QueryOperator.Equal, new List<object>() { "ScrapeEngine" });

            Console.WriteLine($"Time: {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")} Instances in Dynamo: {found.Count}");
            var now = DateTime.UtcNow;

            //Console.WriteLine($"ec2 is instance: {(ec2 != null)}");
            var reservations = await ec2.DescribeAllInstances();

            reservations
                .Where(instance => instance != null && instance.Instances != null).ToList();

            //Console.WriteLine($"Number of Reservations: {reservations.Count}");

            var instances = reservations
                .SelectMany(instance => instance.Instances)
                .Where(instance => instance.State != null && instance.Tags != null)
                .ToList();

            //Console.WriteLine($"Number of Instances: {instances.Count}");

            var engines = instances
                .Where(instance => instance != null && instance.State != null && instance.State.Code == 16 && instance.Tags != null && instance.Tags.Any(tag => tag != null && tag.Key == "Name" && !string.IsNullOrWhiteSpace(tag.Value) && tag.Value.ToLower().Contains("scrape-engine")))
                .ToList();

            Console.WriteLine($"Number of EC2 Engines: {engines.Count}");

            var engineIds = engines.Select(engine => engine.InstanceId).ToList();

            var enginesNotInFound = engineIds.Except(found.Select(record => record.InstanceId)).ToList();

            Console.WriteLine($"Instances Not In HeartBeat: { string.Join(", ", enginesNotInFound)}");
            var expired = found.Where(s => 90d < (now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes).ToList();

            Console.WriteLine($"Killing Instances Not Updating > 1.5 Hour: { string.Join(", ", expired.OrderBy(s => s.LastSignalTimestamp).Select(s => $"{s.InstanceId}({(now - s.LastSignalTimestamp.ToUniversalTime()).TotalMinutes.ToString("0")}min)"))}");

            await ec2.TerminateByIds(expired.Select(hb => hb.InstanceId).ToList());
            foreach(var heartBeat in expired)
            {
                await dynamo.DeleteItem(new Dictionary<string, object>()
                {
                    {nameof(heartBeat.InstanceId), heartBeat.InstanceId },
                    {nameof(heartBeat.Job), heartBeat.Job },
                });
            }
        }
    }
}
