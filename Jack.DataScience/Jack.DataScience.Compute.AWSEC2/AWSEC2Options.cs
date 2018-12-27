using System;
using System.Collections.Generic;

namespace Jack.DataScience.Compute.AWSEC2
{
    public class AWSEC2Options
    {
        public string Key { get; set; }
        public string Secret { get; set; }
        public string Region { get; set; }
        public List<string> InstanceIds { get; set; }
        public List<string> StartIds { get; set; }
        public List<string> StopIds { get; set; }
    }
}
