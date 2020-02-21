using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.AWSAsync
{
    public class AsyncJob
    {
        public string query { get; set; }
        /// <summary>
        /// this can be athena, batch, lambda, step functions
        /// </summary>
        public string type { get; set; }
        /// <summary>
        /// this should not be here as the logic will parse the parameters from the "query" field
        /// </summary>
        //public JObject input { get; set; }
        public int wait { get; set; }
    }
}
