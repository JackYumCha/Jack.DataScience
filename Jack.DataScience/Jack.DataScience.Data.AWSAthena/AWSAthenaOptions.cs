using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.AWSAthena
{
    public class AWSAthenaOptions
    {
        public string Key { get; set; }
        public string Secret { get; set; }
        public string Region { get; set; }
        public string DefaultOutputLocation { get; set; }
    }
}
