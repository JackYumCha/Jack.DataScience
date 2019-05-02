using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.MongoDB.Extensions
{
    public class MongoInsertResult
    {
        public string _id { get; set; }
        public int retry { get; set; }
        public string errmsg { get; set; }
        public MongoWriteResult result { get; set; }
    }
}
