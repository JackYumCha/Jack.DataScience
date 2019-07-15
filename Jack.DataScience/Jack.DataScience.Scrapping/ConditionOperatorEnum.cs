using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Scrapping
{
    public enum ConditionOperatorEnum
    {
        EqualTo,
        NotEqualTo,
        LessThan,
        LessThanOrEqualTo,
        GreaterThan,
        GreaterThanOrEqualTo,
    }

    public static class ConditionOperatorEnumExtensions
    {
        public static ConditionOperatorEnum ParseConditionOperator(this string value)
        {
            switch (value)
            {
                case "=":
                case "==":
                    return ConditionOperatorEnum.EqualTo;
                case ">":
                    return ConditionOperatorEnum.GreaterThan;
                case ">=":
                    return ConditionOperatorEnum.GreaterThanOrEqualTo;
                case "<":
                    return ConditionOperatorEnum.LessThan;
                case "<=":
                    return ConditionOperatorEnum.LessThanOrEqualTo;
                case "!=":
                    return ConditionOperatorEnum.NotEqualTo;
            }
            throw new ScrapingException($"Invalid Condition Operator '{value}'");
        }
    }
       
}
