using System;

namespace Jack.DataScience.Cloud.HeartBeat
{
    public class HeartBeatSignal
    {
        public string InstanceId { get; set; }
        public string Job { get; set; }
        public int RebootTimeout { get; set; }
        public int StopTimeout { get; set; }
        public int TerminateTimeout { get; set; }
        public int LaunchMoreTimeout { get; set; }
        public int Count { get; set; }
        public int LastRun { get; set; }
        public string LastPayload { get; set; }
        public int MaxRun { get; set; }
        public string MaxPayload { get; set; }
        public string LaunchTemplateId { get; set; }
        public DateTime LastSignalTimestamp { get; set; }
        public string Task { get; set; }
        public string Payload { get; set; }
        public string Message { get; set; }
        public string Log { get; set; }
    }
}
