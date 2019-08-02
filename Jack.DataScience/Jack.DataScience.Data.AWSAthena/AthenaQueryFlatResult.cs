using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.AWSAthena
{
    public class AthenaQueryFlatResult
    {
        public List<string> Columns { get; set; }
        public List<List<string>> Data { get; set; }
    }
}
