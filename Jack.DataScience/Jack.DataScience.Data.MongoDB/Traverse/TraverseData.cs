using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{
    public class TraverseData<TVertex, TEdge> where TVertex: DocumentBase where TEdge: EdgeBase
    {
        public List<TEdge> edges { get; set; }
        public List<TVertex> vertices { get; set; }
    }
}
