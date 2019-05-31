using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.MQ.AWSKenesis
{
    public class AWSKinesisOptions
    {
        public string Key { get; set; }
        public string Secret { get; set; }
        public string Region { get; set; }
        public string Stream { get; set; }
    }
}
