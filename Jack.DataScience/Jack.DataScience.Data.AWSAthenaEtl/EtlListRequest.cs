using System;
using System.Collections.Generic;
using System.Text;
using MvcAngular;
namespace Jack.DataScience.Data.AWSAthenaEtl
{
    [AngularType]
    public class EtlListRequest
    {
        public int Index { get; set; }
        public int NumberPerPage { get; set; }
        public List<string> Keywords { get; set; }
    }
}
