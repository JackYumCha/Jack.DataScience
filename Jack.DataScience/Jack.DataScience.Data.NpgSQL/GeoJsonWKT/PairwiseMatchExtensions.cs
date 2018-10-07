using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.Linq;
using System.Threading.Tasks;
using System.Text.RegularExpressions;

namespace Jack.DataScience.DataTypes
{
    public static class PairwiseMatchExtensions
    {
        public static List<string> PairwiseMatch(this string value, string left, string right)
        {
            int currentPosition = 0;
            int nextLeft = value.IndexOf(left, currentPosition);
            int level = 0;
            int start = 0;

            List<string> results = new List<string>();

            while (nextLeft > -1)
            {
                currentPosition = nextLeft + 1;
                if (level == 0)
                {
                    start = nextLeft;
                }
                level += 1;

                nextLeft = value.IndexOf(left, currentPosition);

                if (nextLeft == -1)
                {
                    nextLeft = value.Length; // reached end of string
                }

                int nextRight = value.IndexOf(right, currentPosition);

                if (nextRight == -1) // end of string
                {
                    return results; // no more matches
                }

                if (nextLeft < nextRight)
                {

                }

                while (nextRight < nextLeft)
                {
                    currentPosition = nextRight + 1;
                    level -= 1;

                    if (level > 0)
                    {
                        nextRight = value.IndexOf(right, currentPosition);
                        if (nextRight == -1)
                        {
                            return results; // no more matches
                        }
                    }
                    else
                    {
                        results.Add(value.Substring(start, currentPosition - start));
                        nextRight = value.IndexOf(right, currentPosition);
                        if (nextRight == -1)
                        {
                            return results;
                        }
                    }
                }
            }

            return results;
        }

        private static int RegexIndexOf(this string value, Regex regex, int currentPosition, out Match match)
        {
            match = regex.Match(value, currentPosition);
            return match.Success ? match.Index : -1;
        }

        public static List<PatternPairMatch> PairwisePatternMatch(this string value, Regex left, Regex right, bool expanding = false)
        {
            int currentPosition = 0;
            Match leftMatch;
            int nextLeft = value.RegexIndexOf(left, currentPosition, out leftMatch);
            int level = 0;
            Match startMatch = null;
            int start = 0;

            List<PatternPairMatch> results = new List<PatternPairMatch>();

            while (nextLeft > -1)
            {
                currentPosition = nextLeft + leftMatch.Length;
                if (level == 0)
                {
                    startMatch = leftMatch;
                    start = nextLeft;
                }
                level += 1;

                nextLeft = value.RegexIndexOf(left, currentPosition, out leftMatch);

                if (nextLeft == -1)
                {
                    nextLeft = value.Length; // reached end of string
                }

                Match rightMatch;
                int nextRight = value.RegexIndexOf(right, currentPosition, out rightMatch);

                if (nextRight == -1) // end of string
                {
                    return results; // no more matches
                }

                if (nextLeft < nextRight)
                {

                }

                while (nextRight < nextLeft)
                {
                    currentPosition = nextRight + rightMatch.Length;
                    level -= 1;

                    if (level > 0)
                    {
                        nextRight = value.RegexIndexOf(right, currentPosition, out rightMatch);
                        if (nextRight == -1)
                        {
                            return results; // no more matches
                        }
                    }
                    else
                    {
                        if (expanding)
                        {
                            results.Add(new PatternPairMatch(
                                startMatch,
                                rightMatch,
                                value.Substring(start + startMatch.Length, rightMatch.Index - (start + startMatch.Length)),
                                left,
                                right
                                ));
                        }
                        else
                        {
                            results.Add(new PatternPairMatch(
                                startMatch,
                                rightMatch,
                                value.Substring(start + startMatch.Length, rightMatch.Index - (start + startMatch.Length))
                                ));
                        }

                        nextRight = value.RegexIndexOf(right, currentPosition, out rightMatch);
                        if (nextRight == -1)
                        {
                            return results;
                        }
                    }
                }
            }

            return results;
        }
    }

    public class PatternPairMatch
    {
        public PatternPairMatch(Match left, Match right, string content)
        {
            Left = left;
            Right = right;
            Content = content;
        }

        public PatternPairMatch(Match left, Match right, string content, Regex rgxLeft, Regex rgxRight)
        {
            Left = left;
            Right = right;
            Content = content;
            ExpandBranch(rgxLeft, rgxRight);
        }

        public Match Left { get; private set; }

        public Match Right { get; private set; }

        public string Content { get; private set; }

        public ReadOnlyCollection<PatternPairMatch> Children { get; private set; }

        public int ExpandBranch(Regex left, Regex right)
        {
            Children = new ReadOnlyCollection<PatternPairMatch>(Content.PairwisePatternMatch(left, right, true));
            return Children.Count;
        }
    }


}
