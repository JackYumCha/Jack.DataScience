using System;
using System.Collections.Generic;
using System.Text;
using MvcAngular;

namespace Jack.DataScience.Data.AWSAthenaEtl
{
    /// <summary>
    /// this is the schema of the data
    /// </summary>
    [AngularType]
    public class GenericReportField
    {
        public string Name { get; set; }
        public string Type { get; set; }
        public string FrontEndPipe { get; set; }
        public bool IsDateKey { get; set; }
    }
}
