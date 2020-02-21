using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.AWSAsync
{
    /// <summary>
    /// this state is used for multiple integrations
    /// </summary>
    public class AsyncJobState
    {
        public string id { get; set; }
        /// <summary>
        /// can be Athena, Batch, 
        /// </summary>
        public string type { get; set; }
        public int wait { get; set; }
        public bool success { get; set; }
        public string error { get; set; }
        public bool boolValue { get; set; }
        public long intValue { get; set; }
        public string strValue { get; set; }
        public string state { get; set; }
    }
}
