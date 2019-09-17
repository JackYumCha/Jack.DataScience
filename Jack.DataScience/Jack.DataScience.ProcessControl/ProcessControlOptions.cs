using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.ProcessControl
{
    public class ProcessControlOptions
    {
        public List<string> ProcessesToStart { get; set; }
        public List<string> ProcessesToKillOnError { get; set; }
        public int Retry { get; set; }
    }
}
