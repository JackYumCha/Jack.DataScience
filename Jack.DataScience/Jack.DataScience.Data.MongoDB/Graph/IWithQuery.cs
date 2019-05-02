using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{
    public interface IWithQuery
    {
        Dictionary<string, EdgeQuery> withs { get; }
    }
}
