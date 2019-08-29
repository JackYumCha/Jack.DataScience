using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.LogWrapper
{
    public class LogMessage
    {
        public DateTime Timestamp { get; set; }
        public string LogLevel { get; set; }
        public string Message { get; set; }
    }
}
