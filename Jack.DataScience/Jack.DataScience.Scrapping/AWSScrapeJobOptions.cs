using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Scrapping
{
    public class AWSScrapeJobOptions
    {
        public string JobName { get; set; }
        public string JobARN { get; set; }
        public string QueueARN { get; set; }
        public int JobRate { get; set; }
        public int InvisibilityTime { get; set; }
        public int MaxAttempts { get; set; }
        public int DefaultTTL { get; set; }
        public bool Verbose { get; set; }
        /// <summary>
        /// map s3 file to local in debug mode
        /// </summary>
        public Dictionary<string, string> TestScriptMapping { get; set; }
        public bool TestMode { get; set; }
        public List<ScriptJob> TestQueue { get; set; }
    }

    //public class ScriptMapping
    //{
    //    public string Script { get; set; }
    //    public string File { get; set; }
    //}
}
