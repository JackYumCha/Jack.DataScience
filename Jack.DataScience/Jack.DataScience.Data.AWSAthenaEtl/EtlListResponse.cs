using System;
using System.Collections.Generic;
using System.Text;
using MvcAngular;

namespace Jack.DataScience.Data.AWSAthenaEtl
{
    [AngularType]
    public class EtlListResponse
    {
        public int Index { get; set; }
        public int PageCount { get; set; }
        public int NumberPerPage { get; set; }
        public List<EtlSettings> Settings { get; set; }
    }
}
