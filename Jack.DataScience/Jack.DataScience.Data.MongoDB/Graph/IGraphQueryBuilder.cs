using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{
    public interface IGraphQueryBuilder
    {
        //SortedGraph ToGraph();
        SortedGraph ToSortedGraph();
    }
}
