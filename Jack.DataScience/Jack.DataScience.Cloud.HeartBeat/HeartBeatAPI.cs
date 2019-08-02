using Amazon.DynamoDBv2.DocumentModel;
using Jack.DataScience.Compute.AWSEC2;
using Jack.DataScience.Data.AWSDynamoDB;
using System;
using System.Collections.Generic;
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
        
    }
}
