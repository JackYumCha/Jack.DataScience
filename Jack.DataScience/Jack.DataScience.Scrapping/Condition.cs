using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Jack.DataScience.Scrapping
{
    public class Condition
    {
        public ConditionOperatorEnum Operator { get; set; }
        public int Value { get; set; }
        public bool Test(int value)
        {
            switch (Operator)
            {
                case ConditionOperatorEnum.EqualTo:
                    return value == Value;
                case ConditionOperatorEnum.NotEqualTo:
                    return value != Value;
                case ConditionOperatorEnum.GreaterThan:
                    return value > Value;
                case ConditionOperatorEnum.GreaterThanOrEqualTo:
                    return value >= Value;
                case ConditionOperatorEnum.LessThan:
                    return value < Value;
                case ConditionOperatorEnum.LessThanOrEqualTo:
                    return value <= Value;
            }
            return false;
        }
    }

    public static class ConditionExtensions
    {
        private static readonly Regex rgxCondition = new Regex(@"(>|>=|=|==|<|<=|!=)(-?\d+)");

        public static Condition ParseCondition(this string value)
        {
            var match = rgxCondition.Match(value);
            if (!match.Success) return null;
            return new Condition()
            {
                Operator = match.Groups[1].Value.ParseConditionOperator(),
                Value = int.Parse(match.Groups[2].Value)
            };
        }
    }
}
