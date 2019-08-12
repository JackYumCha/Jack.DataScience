namespace Jack.DataScience.Logging.AWSCloudWatch
{
    public class CloudWatchLogExportEntry
    {
        public string Destination { get; set; }
        public string DestinationPrefix { get; set; }
        public string LogGroupName { get; set; }
        public string LogStreamPrefix { get; set; }
        public int ExportPeriodInMinutes { get; set; }
        public string TaskName { get; set; }
        public string Partition { get; set; }
    }
}
