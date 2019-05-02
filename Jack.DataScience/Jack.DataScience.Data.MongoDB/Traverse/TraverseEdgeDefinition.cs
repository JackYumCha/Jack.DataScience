using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{
    internal class TraverseEdgeDefinition
    {
        public string name { get; set; }
        public Direction direction { get; set; }
    }

    public class EdgeDefinition
    {
        public Type Type { get; set; }
        public Direction Direction { get; set; }
    }
    public class EdgeDefinition<TEdge>: EdgeDefinition where TEdge: EdgeBase
    {
        public EdgeDefinition(Direction Direction)
        {
            this.Direction = Direction;
            Type = typeof(TEdge);
        }
    }

}
