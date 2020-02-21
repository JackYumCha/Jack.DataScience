using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.AWSAsync
{
    public class BatchPayload
    {
        public string name { get; set; }
        public string queue { get; set; }
        public string job { get; set; }
        public Dictionary<string, string> parameters { get; set; }
    }
}
