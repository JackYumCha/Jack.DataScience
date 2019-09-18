using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Jack.DataScience.ProcessControl
{
    public class StreamTimeoutChecker
    {
        public int Timeout { get; set; }
        public Stopwatch Stopwatch { get; set; }
        public string StreamName { get; set; }
    }
}
