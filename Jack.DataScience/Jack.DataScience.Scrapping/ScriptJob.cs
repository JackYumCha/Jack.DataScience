using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Scrapping
{
    public class ScriptJob
    {
        /// <summary>
        /// this should be json file location in s3
        /// </summary>
        public string Script { get; set; }
        /// <summary>
        /// this should be url, or else a unique key
        /// </summary>
        public string Job { get; set; }
        public int Attempts { get; set; }
        /// <summary>
        /// time to live in hours 
        /// </summary>
        public int TTL { get; set; }
        public bool ShouldSchedule { get; set; }
        public ScriptJobStateEnum State { get; set; }
        public JObject Payload { get; set; }
        public DateTime LastSchedule { get; set; }
    }
}
