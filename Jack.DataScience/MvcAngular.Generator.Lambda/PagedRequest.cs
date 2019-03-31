using System;
using System.Collections.Generic;
using System.Text;

namespace MvcAngular.Generator.Lambda
{
    [AngularType]
    public class PagedRequest
    {
        public int NumberPerPage { get; set; }
        public int PageIndex { get; set; }
    }
}
