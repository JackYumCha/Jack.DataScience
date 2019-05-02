using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{
    public class Branch
    {
        public VertexInfo from { get; set; }
        public List<VertexInfo> to { get; set; }
        public override string ToString()
        {
            return $"{from.ToString()} -> {string.Join(", ", to.Select(t => t.ToString()))}";
        }
    }

    public class Branch<TFrom, TEdge, TTo>
    {
        public TFrom From { get; set;}
        public List<Arm<TEdge, TTo>> Arms { get; set; }
    }

    public class Arm<TEdge, TTo>
    {
        public TEdge Edge { get; set; }
        public TTo To { get; set; }
    }
}
