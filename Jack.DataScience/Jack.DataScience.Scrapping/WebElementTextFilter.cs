using OpenQA.Selenium;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Jack.DataScience.Scrapping
{
    public class WebElementTextFilter
    {
        public WebElementTextFilterTypeEnum FilterType { get; set; }
        public Regex Regex { get; set; }
        public string AttributeKey { get; set; }
        public IEnumerable<IWebElement> TextFilter(IEnumerable<IWebElement> elements, IWebDriver driver)
        {
            switch (FilterType)
            {
                case WebElementTextFilterTypeEnum.None:
                    return elements;
                case WebElementTextFilterTypeEnum.Text:
                    return elements.Where(el =>
                    {
                        var text = el.Text;
                        return text != null && Regex.IsMatch(text);
                    });
                case WebElementTextFilterTypeEnum.Attribute:
                    return elements.Where(el =>
                    {
                        var attr = el.GetAttribute(AttributeKey);
                        return attr != null && Regex.IsMatch(attr);
                    });
                case WebElementTextFilterTypeEnum.InnerHTML:
                    return elements.Where(el =>
                    {
                        var inner = el.GetInnerHtml(driver);
                        return inner != null && Regex.IsMatch(inner);
                    });
                case WebElementTextFilterTypeEnum.OuterHTML:
                    return elements.Where(el =>
                    {
                        var outer = el.GetOuterHtml(driver);
                        return outer != null && Regex.IsMatch(outer);
                    });
            }
            throw new ScrapingException($"Unsupported Text Filter Type");
        }

        public static WebElementTextFilter None
        {
            get => new WebElementTextFilter()
            {
                FilterType = WebElementTextFilterTypeEnum.None,
            };
        }
    } 



    public static class WebElementFilterExtensions
    {
        public static WebElementTextFilter BuildFilter(this string filterType, string pattern)
        {
            switch (filterType.ToLower())
            {
                case "none":
                    return WebElementTextFilter.None;
                case "text":
                    return new WebElementTextFilter()
                    {
                        FilterType = WebElementTextFilterTypeEnum.Text,
                        Regex = new Regex(pattern)
                    };
                case "inner":
                    return new WebElementTextFilter()
                    {
                        FilterType = WebElementTextFilterTypeEnum.InnerHTML,
                        Regex = new Regex(pattern)
                    };
                case "outer":
                    return new WebElementTextFilter()
                    {
                        FilterType = WebElementTextFilterTypeEnum.OuterHTML,
                        Regex = new Regex(pattern)
                    };
                default:
                    if(filterType.StartsWith("[") && pattern.EndsWith("]"))
                    {
                        return new WebElementTextFilter()
                        {
                            FilterType = WebElementTextFilterTypeEnum.Attribute,
                            AttributeKey = filterType.Substring(1, filterType.Length - 2),
                            Regex = new Regex(pattern)
                        };
                    }
                    else
                    {
                        throw new ScrapingException($"Invalid Selector Type '{filterType}'");
                    }
            }
        }

        public static IEnumerable<IWebElement> TextFilter(this IEnumerable<IWebElement> elements, WebElementTextFilter filter, IWebDriver driver)
        {
            return filter.TextFilter(elements, driver);
        }
    }
}
