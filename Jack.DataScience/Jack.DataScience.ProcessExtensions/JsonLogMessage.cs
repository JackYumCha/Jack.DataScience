using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.ProcessExtensions
{
    public class JsonLogMessage
    {
        public string JsonLogType { get; set; } = "[JsonLogType]";
        public string Message { get; set; }
        public string Json { get; set; }
    }
}
