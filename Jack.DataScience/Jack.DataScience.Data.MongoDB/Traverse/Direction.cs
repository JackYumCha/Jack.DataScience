using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{
    public enum Direction
    {
        Any = 0,
        Back = -1,
        Forth = 1
    }

    public static class DirectionExtensions
    {
        public static string ToSymbol(this Direction direction)
        {
            switch (direction)
            {
                case Direction.Any: return "";
                case Direction.Back: return "-";
                case Direction.Forth: return "+";
            }
            return "";
        }
    }
}
