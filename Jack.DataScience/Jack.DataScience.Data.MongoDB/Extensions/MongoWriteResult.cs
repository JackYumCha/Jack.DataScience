using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.MongoDB.Extensions
{
    public class MongoWriteResult
    {
        public int nInserted { get; set; }
        public MongoError writeConcernError { get; set; }
        public MongoError writeError { get; set; }
    }
}
