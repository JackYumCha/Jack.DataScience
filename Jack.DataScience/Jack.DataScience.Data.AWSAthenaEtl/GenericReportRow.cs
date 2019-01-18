using System;
using System.Collections.Generic;
using System.Text;
using MvcAngular;

namespace Jack.DataScience.Data.AWSAthenaEtl
{
    [AngularType]
    public class GenericReportRow
    {
        public int Date { get; set; }
        public List<object> Items { get; set; }
    }
}
