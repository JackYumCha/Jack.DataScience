using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.ProcessControl
{
    public class ProcessMonitorOptions
    {
        public string ProcessPath { get; set; }
        public List<string> Arguments { get; set; }
        public string StreamName { get; set; }
    }
}
