using System;

namespace Jack.DataScience.Algorithms.UnionFind
{
    public class DisjointSet
    {
        public int[] parent, rank;
        public int count, size;
        public DisjointSet(int size)
        {
            this.size = size;
            parent = new int[size];
            rank = new int[size];
            count = size;
            for (int i = 0; i < size; i++) parent[i] = i;
        }

        public int Find(int x)
        {
            if (x != parent[x]) parent[x] = Find(parent[x]);
            return parent[x];
        }

        public bool Union(int x, int y)
        {
            if (x == y) return false;
            int px = Find(x), py = Find(y);
            if (px == py) return false;
            if (rank[px] > rank[py]) parent[py] = px;
            else if (rank[px] < rank[py]) parent[px] = py;
            else
            {
                rank[px]++;
                parent[py] = px;
            }
            count--;
            return true;
        }
    }
}
