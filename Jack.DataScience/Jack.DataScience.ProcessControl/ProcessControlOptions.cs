using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.ProcessControl
{
    public class ProcessControlOptions
    {
        /// <summary>
        /// Processes and Arguments
        /// </summary>
        public List<ProcessMonitorOptions> ProcessesToStart { get; set; }
        public List<string> ProcessesToKillOnError { get; set; }
        public int Retry { get; set; }
        public int Interval { get; set; }
    }
}
