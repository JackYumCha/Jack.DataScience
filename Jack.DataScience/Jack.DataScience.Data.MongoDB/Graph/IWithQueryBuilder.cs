using System;

namespace Jack.DataScience.Data.MongoDB
{
    public interface IWithQueryBuilder
    {
        IWithQuery Query { get; }
        IGraphQueryBuilder GraphQueryBuilder { get; }
        //SortedGraph ToGraph();
        SortedGraph ToSortedGraph();
        Type VertexType { get; }
    }
}
