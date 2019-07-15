using Autofac;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using Newtonsoft.Json.Converters;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium.Support.UI;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using ExpectedConditions = SeleniumExtras.WaitHelpers.ExpectedConditions;

namespace Jack.DataScience.Scrapping
{
    public static class BrowserOperationExtensions
    {

        public static IEnumerable<IWebElement> RunOperation(
            this BrowserOperation operation, 
            IWebDriver driver, 
            Dictionary<string, string> data,
            Dictionary<string, Action<IEnumerable<IWebElement>>> jobs,
            Dictionary<string, List<IWebElement>> references,
            Dictionary<string, JObject> jsons,
            Dictionary<string, BrowserOperation> functions,
            int level,
            IComponentContext componentContext)
        {
            List<IWebElement> results = new List<IWebElement>();
            var options = componentContext.Resolve<AWSScrapeJobOptions>();
            var parameters = operation.GetParameters(data);
            bool shouldRunThen = true;
            string stepInfo = $"({level}) -> {operation.Action}({string.Join(", ", parameters)}) {(string.IsNullOrWhiteSpace(operation.Label) ? "" : operation.Label)}";
            if(options.Verbose || !string.IsNullOrWhiteSpace(operation.Label)) Console.WriteLine($"{(operation.Disabled ? "[Disabled] ":"")}{stepInfo}");
            if (operation.Disabled) return results;
            try
            {
                switch (operation.Action)
                {
                    case ActionTypeEnum.Null:
                        throw new ScrapingException("No valid Action was parsed from the json file. Please check your json syntax.");
                    case ActionTypeEnum.GoTo:
                        driver.GoTo(parameters);
                        break;
                    case ActionTypeEnum.Wait:
                        driver.Wait(parameters, results);
                        break;
                    case ActionTypeEnum.By:
                        ByElements(parameters, null, driver, results);
                        break;
                    case ActionTypeEnum.LoopWhen:
                        shouldRunThen = LoopWhen(operation, parameters, null, data, jobs, references, jsons, functions, driver, level, componentContext);
                        break;
                    case ActionTypeEnum.LoopUntil:
                        shouldRunThen = LoopUntil(operation, parameters, null, data, jobs, references, jsons, functions, driver, level, componentContext);
                        break;
                    case ActionTypeEnum.SwitchBy:
                        shouldRunThen = SwitchBy(operation, parameters, null, data, jobs, references, jsons, functions, driver, level, componentContext);
                        break;
                    case ActionTypeEnum.Click:
                        throw new ScrapingException($"{nameof(ActionTypeEnum.Click)} is not avaliable on {nameof(IWebDriver)}.");
                    case ActionTypeEnum.ScrollIntoView:
                        throw new ScrapingException($"{nameof(ActionTypeEnum.ScrollIntoView)} is not avaliable on {nameof(IWebDriver)}.");
                    case ActionTypeEnum.ScrollTo:
                        driver.ScrollTo(parameters);
                        break;
                    case ActionTypeEnum.SendKeys:
                        throw new ScrapingException($"{nameof(ActionTypeEnum.SendKeys)} is not avaliable on {nameof(IWebDriver)}.");
                    case ActionTypeEnum.SkipTake:
                        throw new ScrapingException($"{nameof(ActionTypeEnum.SkipTake)} is not avaliable on {nameof(IWebDriver)}.");
                    case ActionTypeEnum.AttrRegex:
                        throw new ScrapingException($"{nameof(ActionTypeEnum.AttrRegex)} is not avaliable on {nameof(IWebDriver)}.");
                    case ActionTypeEnum.InnerRegex:
                        throw new ScrapingException($"{nameof(ActionTypeEnum.InnerRegex)} is not avaliable on {nameof(IWebDriver)}.");
                    case ActionTypeEnum.OuterRegex:
                        throw new ScrapingException($"{nameof(ActionTypeEnum.OuterRegex)} is not avaliable on {nameof(IWebDriver)}.");
                    case ActionTypeEnum.Put:
                        Put(data, parameters, null, driver);
                        break;
                    case ActionTypeEnum.Collect:
                        Collect(data, parameters, null, driver);
                        break;
                    case ActionTypeEnum.SplitOne:
                        shouldRunThen = data.SplitOne(parameters);
                        break;
                    case ActionTypeEnum.LoopSplitOne:
                        {
                            while (data.SplitOne(parameters))
                            {
                                operation.RunDriverThen(driver, data, jobs, references, jsons, functions, level, componentContext);
                            }
                            shouldRunThen = false;
                        }
                        break;
                    case ActionTypeEnum.Yield:
                        throw new ScrapingException($"{nameof(ActionTypeEnum.Yield)} is not avaliable on {nameof(IWebDriver)}.");
                    case ActionTypeEnum.Fetch:
                        {
                            if (references.ContainsKey(parameters[0]))
                            {
                                results.AddRange(references[parameters[0]]);
                            }
                        }
                        break;
                    case ActionTypeEnum.JsonNew:
                        jsons.JsonNew(parameters);
                        break;
                    case ActionTypeEnum.JsonSet:
                        jsons.JsonSet(parameters);
                        break;
                    case ActionTypeEnum.JsonUnset:
                        jsons.JsonUnset(parameters);
                        break;
                    case ActionTypeEnum.JsonPush:
                        jsons.JsonPush(parameters);
                        break;
                    case ActionTypeEnum.JsonAdd:
                        jsons.JsonAdd(parameters);
                        break;
                    case ActionTypeEnum.JsonAs:
                        jsons.JsonAs(parameters);
                        break;
                    case ActionTypeEnum.JsonDelete:
                        jsons.JsonDelete(parameters);
                        break;
                    case ActionTypeEnum.JsonDeleteWhere:
                        jsons.JsonDeleteWhere(parameters);
                        break;
                    case ActionTypeEnum.JsonSave:
                        jsons.JsonSave(parameters, componentContext);
                        break;
                    case ActionTypeEnum.Log:
                        parameters.Log();
                        break;
                    case ActionTypeEnum.LogData:
                        data.LogData();
                        break;
                    case ActionTypeEnum.LogJson:
                        parameters.LogJson(jsons);
                        break;
                    case ActionTypeEnum.LogJsonWhere:
                        parameters.LogJsonWhere(jsons);
                        break;
                    case ActionTypeEnum.Break:
                        Debugger.Break();
                        break;
                    case ActionTypeEnum.Function:
                        shouldRunThen = parameters.SaveFunction(operation, functions);
                        break;
                    case ActionTypeEnum.Call:
                        shouldRunThen = parameters.CallFunction(null, data, jobs, references, jsons, functions, driver, level, componentContext);
                        break;
                    case ActionTypeEnum.JobNew:
                        JobNew(jsons, parameters);
                        break;
                    case ActionTypeEnum.JobUpdate:
                        JobUpdate(jsons, parameters, componentContext);
                        break;
                }
            }
            catch (Exception ex)
            {
                throw new ScrapingException($"[Error] {stepInfo}", ex);
            }
            if(shouldRunThen)
            {
                try
                {
                    operation.RunThen(results, data, jobs, references, jsons, functions, driver, level, componentContext);
                }
                catch (Exception ex)
                {
                    throw new ScrapingException($"[Sub Steps Error] at {stepInfo}", ex);
                }
            }
            return results;
        }

        public static List<IWebElement> RunOperation(
            this BrowserOperation operation,
            IWebElement parent,
            Dictionary<string, string> data,
            Dictionary<string, Action<IEnumerable<IWebElement>>> jobs,
            Dictionary<string, List<IWebElement>> references,
            Dictionary<string, JObject> jsons,
            Dictionary<string, BrowserOperation> functions,
            IWebDriver driver, 
            int level,
            IComponentContext componentContext)
        {
            List<IWebElement> results = new List<IWebElement>();
            var options = componentContext.Resolve<AWSScrapeJobOptions>();
            var parameters = operation.GetParameters(data);
            bool shouldRunThen = true;
            string stepInfo = $"({level}) -> {operation.Action}({string.Join(", ", parameters)}) {(string.IsNullOrWhiteSpace(operation.Label) ? "" : operation.Label)}";
            if (options.Verbose || !string.IsNullOrWhiteSpace(operation.Label)) Console.WriteLine($"{(operation.Disabled ? "[Disabled] " : "")}{stepInfo}");
            if (operation.Disabled) return results;
            try
            {
                switch (operation.Action)
                {
                    case ActionTypeEnum.Null:
                        throw new ScrapingException("No valid Action was parsed from the json file. Please check your json syntax.");
                    case ActionTypeEnum.GoTo:
                        driver.GoTo(parameters);
                        break;
                    case ActionTypeEnum.Wait:
                        driver.Wait(parameters, results);
                        break;
                    case ActionTypeEnum.LoopWhen:
                        shouldRunThen = LoopWhen(operation, parameters, parent, data, jobs, references, jsons, functions, driver, level, componentContext);
                        break;
                    case ActionTypeEnum.LoopUntil:
                        shouldRunThen = LoopUntil(operation, parameters, parent, data, jobs, references, jsons, functions, driver, level, componentContext);
                        break;
                    case ActionTypeEnum.SwitchBy:
                        shouldRunThen = SwitchBy(operation, parameters, parent, data, jobs, references, jsons, functions, driver, level, componentContext);
                        break;
                    case ActionTypeEnum.By:
                        ByElements(parameters, parent, driver, results);
                        break;
                    case ActionTypeEnum.Click:
                        {
                            parent.Click();
                            results.Add(parent);
                        }
                        break;
                    case ActionTypeEnum.ScrollIntoView:
                        {
                            parent.ScrollIntoView(driver);
                            results.Add(parent);
                        }
                        break;
                    case ActionTypeEnum.ScrollTo:
                        driver.ScrollTo(parameters);
                        break;
                    case ActionTypeEnum.SendKeys:
                        {
                            parent.SendKeys(parameters[0]);
                            results.Add(parent);
                        }
                        break;
                    case ActionTypeEnum.Yield:
                        {
                            if (references.ContainsKey(parameters[0]))
                            {
                                references[parameters[0]].Add(parent);
                            }
                            else
                            {
                                references.Add(parameters[0], new List<IWebElement>() { parent });
                            }
                            results.Add(parent);
                        }
                        break;
                    case ActionTypeEnum.Fetch:
                        {
                            if (references.ContainsKey(parameters[0]))
                            {
                                results.AddRange(references[parameters[0]]);
                            }
                        }
                        break;
                    case ActionTypeEnum.AttrRegex:
                        {
                            var attr = parent.GetAttribute(parameters[0]);
                            if (attr != null && Regex.IsMatch(attr, parameters[1]))
                            {
                                results.Add(parent);
                            }
                        }
                        break;
                    case ActionTypeEnum.InnerRegex:
                        {
                            var inner = parent.GetInnerHtml(driver);
                            if (inner != null && Regex.IsMatch(inner, parameters[1]))
                            {
                                results.Add(parent);
                            }
                        }
                        break;
                    case ActionTypeEnum.OuterRegex:
                        {
                            var outer = parent.GetOuterHtml(driver);
                            if (outer != null && Regex.IsMatch(outer, parameters[1]))
                            {
                                results.Add(parent);
                            }
                        }
                        break;
                    case ActionTypeEnum.Put:
                        Put(data, parameters, parent, driver);
                        break;
                    case ActionTypeEnum.Collect:
                        Collect(data, parameters, parent, driver);
                        break;
                    case ActionTypeEnum.SplitOne:
                        shouldRunThen = data.SplitOne(parameters);
                        break;
                    case ActionTypeEnum.LoopSplitOne:
                        {
                            while (data.SplitOne(parameters))
                            {
                                operation.RunDriverThen(driver, data, jobs, references, jsons, functions, level, componentContext);
                            }
                            shouldRunThen = false;
                        }
                        break;
                    case ActionTypeEnum.JsonNew:
                        jsons.JsonNew(parameters);
                        break;
                    case ActionTypeEnum.JsonSet:
                        jsons.JsonSet(parameters);
                        break;
                    case ActionTypeEnum.JsonUnset:
                        jsons.JsonUnset(parameters);
                        break;
                    case ActionTypeEnum.JsonPush:
                        jsons.JsonPush(parameters);
                        break;
                    case ActionTypeEnum.JsonAdd:
                        jsons.JsonAdd(parameters);
                        break;
                    case ActionTypeEnum.JsonAs:
                        jsons.JsonAs(parameters);
                        break;
                    case ActionTypeEnum.JsonDelete:
                        jsons.JsonDelete(parameters);
                        break;
                    case ActionTypeEnum.JsonDeleteWhere:
                        jsons.JsonDeleteWhere(parameters);
                        break;
                    case ActionTypeEnum.JsonSave:
                        jsons.JsonSave(parameters, componentContext);
                        break;
                    case ActionTypeEnum.Log:
                        parameters.Log();
                        break;
                    case ActionTypeEnum.LogData:
                        data.LogData();
                        break;
                    case ActionTypeEnum.LogJson:
                        parameters.LogJson(jsons);
                        break;
                    case ActionTypeEnum.LogJsonWhere:
                        parameters.LogJsonWhere(jsons);
                        break;
                    case ActionTypeEnum.Break:
                        Debugger.Break();
                        break;
                    case ActionTypeEnum.Function:
                        shouldRunThen = parameters.SaveFunction(operation, functions);
                        break;
                    case ActionTypeEnum.Call:
                        shouldRunThen = parameters.CallFunction(parent, data, jobs, references, jsons, functions, driver, level, componentContext);
                        break;
                    case ActionTypeEnum.JobNew:
                        JobNew(jsons, parameters);
                        break;
                    case ActionTypeEnum.JobUpdate:
                        JobUpdate(jsons, parameters, componentContext);
                        break;
                }
            }
            catch (Exception ex)
            {
                throw new ScrapingException($"[Error] {stepInfo}", ex);
            }
            if(shouldRunThen)
            {
                try
                {
                    operation.RunThen(results, data, jobs, references, jsons, functions, driver, level, componentContext);
                }
                catch (Exception ex)
                {
                    throw new ScrapingException($"[Sub Steps Error] at {stepInfo}", ex);
                }
            }
            return results;
        }

        private static void GoTo(this IWebDriver driver, List<string> parameters)
        {
            driver.Url = parameters[0];
            driver.Navigate();
        }

        private static void ScrollTo(this IWebDriver driver, List<string> parameters)
        {
            string x = parameters[0], y = parameters[1];
            driver.WindowScrollTo(x, y);
        }

        private static void ByElements(this List<string> parameters, IWebElement parent, IWebDriver driver, List<IWebElement> results)
        {
            if (parameters.Count >= 2)
            {
                var selectorType = parameters[0].ToLower();
                var selector = parameters[1];
                By by = selectorType.Selector(selector);
                if (parent == null || (parameters.Count >= 3 && parameters[2].ToLower() == "root"))
                {
                    var elements = driver.FindElements(by);
                    results.AddRange(elements);
                }
                else
                {
                    var elements = parent.FindElements(by);
                    results.AddRange(elements);
                }
            }
        }

        private static By Selector(this string selectorType, string selector)
        {
            switch (selectorType.ToLower())
            {
                case "css":
                    return By.CssSelector(selector);
                case "id":
                    return By.Id(selector);
                case "xpath":
                    return By.XPath(selector);
                case "class":
                    return By.ClassName(selector);
                default:
                    throw new ScrapingException($"Invalid Selector Type '{selectorType}'");
            }
        }

        private static void Wait(this IWebDriver driver, List<string> parameters, IList<IWebElement> results)
        {
            int seconds = 3;
            if (parameters.Count >= 1)
            {
                int.TryParse(parameters[0], out seconds);
            }
            if (parameters.Count >= 4)
            {
                var condition = parameters[1].ToLower();
                var selectorType = parameters[2];
                var selector = parameters[3];
                var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(seconds));
                By by = selectorType.Selector(selector);
                IWebElement found;
                switch (condition)
                {
                    case "exists":
                        found = wait.Until(ExpectedConditions.ElementExists(by));
                        results.Add(found);
                        break;
                    case "visible":
                        found = wait.Until(ExpectedConditions.ElementIsVisible(by));
                        results.Add(found);
                        break;
                    case "clickable":
                        found = wait.Until(ExpectedConditions.ElementToBeClickable(by));
                        results.Add(found);
                        break;
                    case "selected":
                        wait.Until(ExpectedConditions.ElementToBeSelected(by));
                        break;
                    case "invisible":
                        wait.Until(ExpectedConditions.InvisibilityOfElementLocated(by));
                        break;
                }
            }
            else
            {
                var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(seconds + 5));
                wait.PollingInterval = TimeSpan.FromSeconds(seconds);
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();
                wait.Until(d => stopwatch.ElapsedMilliseconds >= seconds * 1000);
            }
        }

        private static bool LoopWhen(this BrowserOperation operation,
            List<string> parameters,
            IWebElement parent,
            Dictionary<string, string> data,
            Dictionary<string, Action<IEnumerable<IWebElement>>> jobs,
            Dictionary<string, List<IWebElement>> references,
            Dictionary<string, JObject> jsons,
            Dictionary<string, BrowserOperation> functions,
            IWebDriver driver,
            int level,
            IComponentContext componentContext)
        {
            List<IWebElement> looping = new List<IWebElement>();
            if (parameters.Count > 3)
            {
                int count = 1;
                if (!int.TryParse(parameters[0], out count))
                {
                    throw new ScrapingException($"The 1st parameter of {nameof(ActionTypeEnum.LoopWhen)} must be a number. It currently is {parameters[0]}");
                }
                var selectorType = parameters[1];
                var selector = parameters[2];
                Condition condition = parameters[3].ParseCondition();
                if (condition == null)
                {
                    throw new ScrapingException($"The 4th parameter of {nameof(ActionTypeEnum.LoopWhen)} must be a Condition Expression. It currently is {parameters[3]}");
                }
                var filter = WebElementTextFilter.None;
                if (parameters.Count >= 7)
                {
                    var textFilter = parameters[4];
                    var pattern = parameters[5];
                    filter = textFilter.BuildFilter(pattern);
                }
                By by = selectorType.Selector(selector);
                bool shouldLoop = true;
                while (shouldLoop && count > 0)
                {
                    var found = parent == null ? driver.FindElements(by).ToList() : parent.FindElements(by).ToList();
                    found = found.TextFilter(filter, driver).ToList();
                    shouldLoop = condition.Test(found.Count);
                    if (shouldLoop)
                    {
                        if (found.Any())
                        {
                            try
                            {
                                operation.RunThen(found, data, jobs, references, jsons, functions, driver, level, componentContext);
                            }
                            catch (Exception ex)
                            {
                                throw new ScrapingException($"[LoopWhen Error] at Element", ex);
                            }
                        }
                        else
                        {
                            try
                            {
                                operation.RunDriverThen(driver, data, jobs, references, jsons, functions, level, componentContext);
                            }
                            catch (Exception ex)
                            {
                                throw new ScrapingException($"[LoopWhen Error] at Driver", ex);
                            }
                        }
                    }
                    count--;
                }
            }
            else
            {
                throw new ScrapingException($"{nameof(ActionTypeEnum.LoopWhen)} must have 4 parameters. It currently has {parameters.Count}.");
            }
            return false;
        }

        private static bool LoopUntil(this BrowserOperation operation,
            List<string> parameters,
            IWebElement parent,
            Dictionary<string, string> data,
            Dictionary<string, Action<IEnumerable<IWebElement>>> jobs,
            Dictionary<string, List<IWebElement>> references,
            Dictionary<string, JObject> jsons,
            Dictionary<string, BrowserOperation> functions,
            IWebDriver driver,
            int level,
            IComponentContext componentContext)
        {
            List<IWebElement> looping = new List<IWebElement>();
            if (parameters.Count > 3)
            {
                int count = 1;
                if (!int.TryParse(parameters[0], out count))
                {
                    throw new ScrapingException($"The 1st parameter of {nameof(ActionTypeEnum.LoopWhen)} must be a number. It currently is {parameters[0]}");
                }
                var selectorType = parameters[1];
                var selector = parameters[2];
                var condition = parameters[3].ParseCondition();
                if (condition == null)
                {
                    throw new ScrapingException($"The 4th parameter of {nameof(ActionTypeEnum.LoopWhen)} must be a Condition Expression. It currently is {parameters[3]}");
                }
                var filter = WebElementTextFilter.None;
                if (parameters.Count >= 7)
                {
                    var textFilter = parameters[4];
                    var pattern = parameters[5];
                    filter = textFilter.BuildFilter(pattern);
                }

                By by = selectorType.Selector(selector);
                bool shouldNotLoop = true;
                do
                {
                    var found = parent == null ? driver.FindElements(by).ToList() : parent.FindElements(by).ToList();
                    found = found.TextFilter(filter, driver).ToList();
                    shouldNotLoop = condition.Test(found.Count);
                    if (!shouldNotLoop)
                    {
                        if (found.Any())
                        {
                            try
                            {
                                operation.RunThen(found, data, jobs, references, jsons, functions, driver, level, componentContext);
                            }
                            catch (Exception ex)
                            {
                                throw new ScrapingException($"[LoopUntil Error] at Element", ex);
                            }
                        }
                        else
                        {
                            try
                            {
                                operation.RunDriverThen(driver, data, jobs, references, jsons, functions, level, componentContext);
                            }
                            catch (Exception ex)
                            {
                                throw new ScrapingException($"[LoopUntil Error] at Driver", ex);
                            }
                        }
                    }
                    count--;
                } while (!shouldNotLoop && count > 0);
            }
            else
            {
                throw new ScrapingException($"{nameof(ActionTypeEnum.LoopUntil)} must have 4 parameters. It currently has {parameters.Count}.");
            }
            return false;
        }

        private static bool SwitchBy(this BrowserOperation operation,
            List<string> parameters,
            IWebElement parent,
            Dictionary<string, string> data,
            Dictionary<string, Action<IEnumerable<IWebElement>>> jobs,
            Dictionary<string, List<IWebElement>> references,
            Dictionary<string, JObject> jsons,
            Dictionary<string, BrowserOperation> functions,
            IWebDriver driver,
            int level,
            IComponentContext componentContext)
        {
            if (parameters.Count > 3)
            {
                var selectorType = parameters[0];
                var selector = parameters[1];
                var filter = WebElementTextFilter.None;
                var textFilter = parameters[2];
                var pattern = parameters[3];
                filter = textFilter.BuildFilter(pattern);

                By by = selectorType.Selector(selector);
                var found = parent == null ? driver.FindElements(by).ToList() : parent.FindElements(by).ToList();
                found = found.TextFilter(filter, driver).ToList();
                int count = found.Count;
                int j = 0;
                for (int i = 4; i < parameters.Count; i++)
                {
                    var condition = parameters[i].ParseCondition();
                    if (condition.Test(count))
                    {
                        if(operation.Driver || found.Count == 0)
                        {
                            operation.Then[j].RunOperation(driver, data, jobs, references, jsons, functions, level + 1, componentContext);
                        }
                        else
                        {
                            operation.Then[j].RunOperation(found, data, jobs, references, jsons, functions, driver, level + 1, componentContext);
                        }
                        break;
                    }
                    j++;
                }
            }
            else
            {
                throw new ScrapingException($"{nameof(ActionTypeEnum.SwitchBy)} must have 4 or more parameters. It currently has {parameters.Count}.");
            }
            return false;
        }
        private static bool SaveFunction(this List<string> parameters, BrowserOperation operation, Dictionary<string, BrowserOperation> functions)
        {
            var functionName = parameters[0];
            if (functions.ContainsKey(functionName))
            {
                functions[functionName] = operation;
            }
            else
            {
                functions.Add(functionName, operation);
            }
                return false;
        }

        private static bool CallFunction(this List<string> parameters,
            IWebElement parent,
            Dictionary<string, string> data,
            Dictionary<string, Action<IEnumerable<IWebElement>>> jobs,
            Dictionary<string, List<IWebElement>> references,
            Dictionary<string, JObject> jsons,
            Dictionary<string, BrowserOperation> functions,
            IWebDriver driver,
            int level,
            IComponentContext componentContext)
        {
            var functionName = parameters[0];
            if (functions.ContainsKey(functionName))
            {
                var operation = functions[functionName];
                if (parent == null)
                {
                    try
                    {
                        operation.RunDriverThen(driver, data, jobs, references, jsons, functions, level, componentContext);
                    }
                    catch (Exception ex)
                    {
                        throw new ScrapingException($"[Function({functionName}) Error] at Driver", ex);
                    }
                }
                else
                {
                    try
                    {
                        var elements = new List<IWebElement>() { parent };
                        operation.RunThen(elements, data, jobs, references, jsons, functions, driver, level, componentContext);
                    }
                    catch (Exception ex)
                    {
                        throw new ScrapingException($"[Function({functionName}) Error] at Elements", ex);
                    }
                }
            }
            return false;
        }


        private static void Put(Dictionary<string, string> data, List<string> parameters, IWebElement element, IWebDriver driver)
        {
            var key = parameters[0];
            var type = parameters[1];
            var value = "";
            if(type.StartsWith("[") && type.EndsWith("]"))// attribute
            {
                var attributeKey = type.Substring(1, type.Length - 2);
                value = element.GetAttribute(attributeKey);
            }
            else if (type.StartsWith("=")) // allow set value to the dictionary
            {
                value = type.Substring(1);
            }
            else
            {
                switch (type.ToLower())
                {
                    case "text":
                        value = element.Text;
                        break;
                    case "inner":
                        value = element.GetInnerHtml(driver);
                        break;
                    case "outer":
                        value = element.GetOuterHtml(driver);
                        break;
                }
            }
            var separator = ",";
            if (parameters.Count >= 3)
            {
                separator = parameters[2];
            }

            if (parameters.Count >= 4)
            {
                var pattern = parameters[3];

                var matchedValue = string.Join(separator, Regex.Matches(value, pattern).OfType<Match>().Select(m =>
                {
                    if (m.Groups.Count > 1)
                    {
                        return string.Join(separator, m.Groups.OfType<Group>().Skip(1).Select(g => g.Value));
                    }
                    else
                    {
                        return m.Value;
                    }
                }));

                if (data.ContainsKey(key))
                {
                    data[key] = matchedValue;
                }
                else
                {
                    data.Add(key, matchedValue);
                }
            }
            else
            {
                if (data.ContainsKey(key))
                {
                    data[key] = value;
                }
                else
                {
                    data.Add(key, value);
                }
            }
        }
        private static void Collect(this Dictionary<string, string> data, List<string> parameters, IWebElement element, IWebDriver driver)
        {
            var key = parameters[0];
            var type = parameters[1];
            var value = "";
            if (type.StartsWith("[") && type.EndsWith("]"))// attribute
            {
                var attributeKey = type.Substring(1, type.Length - 2);
                value = element.GetAttribute(attributeKey);
            }
            else if (type.StartsWith("=")) // allow set value to the dictionary
            {
                value = value.Substring(1);
            }
            else
            {
                switch (type.ToLower())
                {
                    case "text":
                        value = element.Text;
                        break;
                    case "inner":
                        value = element.GetInnerHtml(driver);
                        break;
                    case "outer":
                        value = element.GetOuterHtml(driver);
                        break;
                }
            }
            var separator = ",";
            if (parameters.Count >= 3)
            {
                separator = parameters[2];
            }

            if (parameters.Count >= 4)
            {
                var pattern = parameters[3];
                var matchedValue = string.Join(separator, Regex.Matches(value, pattern).OfType<Match>().Select(m =>
                {
                    if(m.Groups.Count > 1)
                    {
                        return string.Join(separator, m.Groups.OfType<Group>().Skip(1).Select(g => g.Value));
                    }
                    else
                    {
                        return m.Value;
                    }
                }));
                if (data.ContainsKey(key))
                {
                    data[key] += separator + matchedValue;
                }
                else
                {
                    data.Add(key, matchedValue);
                }
            }
            else
            {
                if (data.ContainsKey(key))
                {
                    data[key] += separator + value;
                }
                else
                {
                    data.Add(key, value);
                }
            }
        }

        private static bool SplitOne(this Dictionary<string, string> data, List<string> parameters)
        {
            var key = parameters[0];
            var target = parameters[1];
            var separator = parameters[2];
            if (!data.ContainsKey(key))
            {
                data.Add(key, "");
            }
            var value = data[key];
            if (value == null) return false;
            var items = value.Split(new string[] { separator }, StringSplitOptions.RemoveEmptyEntries);
            if (items.Any())
            {
                if (data.ContainsKey(target))
                {
                    data[target] = items.First();
                }
                else
                {
                    data.Add(target, items.First());
                }
                data[key] = string.Join(separator, items.Skip(1));
                return true;
            }
            else
            {
                return false;
            }
        }

        private static void Log(this List<string> parameters)
        {
            foreach (var parameter in parameters)
            {
                Console.WriteLine(parameter);
            }
        }

        private static void LogData(this Dictionary<string, string> data)
        {
            Console.WriteLine($"Begin Log Data: {data.Count} items");
            foreach (var kvp in data)
            {
                Console.WriteLine($"{kvp.Key} -> {kvp.Value}");
            }
            Console.WriteLine($"End Log Data: {data.Count} items");
        }

        private static void LogJson(this List<string> parameters, Dictionary<string, JObject> jsons)
        {
            foreach (var key in parameters)
            {
                if (jsons.ContainsKey(key))
                {
                    Console.WriteLine($"JSON[{key}]:");
                    Console.WriteLine(JsonConvert.SerializeObject(jsons[key], Formatting.Indented));
                }
                else
                {
                    Console.WriteLine($"JSON[{key}]: null");
                }
            }
        }

        private static void LogJsonWhere(this List<string> parameters, Dictionary<string, JObject> jsons)
        {
            var pattern = parameters[0];
            foreach (var key in jsons.Keys.ToArray())
            {
                if (Regex.IsMatch(key, pattern))
                {
                    Console.WriteLine($"JSON[{key}]:");
                    Console.WriteLine(JsonConvert.SerializeObject(jsons[key], Formatting.Indented));
                }
            }
        }

        private static void JsonNew(this Dictionary<string, JObject> jsons, List<string> parameters)
        {
            var key = parameters[0];
            if (jsons.ContainsKey(key))
            {
                jsons[key] = new JObject();
            }
            else
            {
                jsons.Add(key, new JObject());
            }
        }


        private static void JsonSet(this Dictionary<string, JObject> jsons, List<string> parameters)
        {
            var key = parameters[0];
            var field = parameters[1];
            var type = parameters[2].ToLower();
            var value = parameters[3];
            JObject obj = null;
            if (!jsons.ContainsKey(key))
            {
                jsons.Add(key, new JObject());
            }
            obj = jsons[key];
            var prop = obj.Property(field);
            switch (type)
            {
                case "string":
                case "str":
                    {
                        if (prop == null)
                        {
                            obj.Add(field, JToken.FromObject(value));
                        }
                        else
                        {
                            prop.Value = JToken.FromObject(value);
                        }
                    }
                    break;
                case "integer":
                case "int":
                    {
                        int d = 0;
                        int.TryParse(value, out d);
                        if (prop == null)
                        {
                            obj.Add(field, JToken.FromObject(d));
                        }
                        else
                        {
                            prop.Value = JToken.FromObject(d);
                        }
                    }
                    break;
                case "float":
                case "double":
                case "dbl":
                    {
                        double d = 0d;
                        double.TryParse(value, out d);
                        if (prop == null)
                        {
                            obj.Add(field, JToken.FromObject(d));
                        }
                        else
                        {
                            prop.Value = JToken.FromObject(d);
                        }
                    }
                    break;
                case "bool":
                case "boolean":
                    {
                        bool b = false;
                        if(value != null)
                        {
                            var lower = value.ToLower();
                            if(lower == "true" || lower == "1" || lower == "ok" || lower == "accepted" || lower == "yes")
                            {
                                b = true;
                            }
                        }
                        if (prop == null)
                        {
                            obj.Add(field, JToken.FromObject(b));
                        }
                        else
                        {
                            prop.Value = JToken.FromObject(b);
                        }
                    }
                    break;
                case "null":
                    {
                        if (prop == null)
                        {
                            obj.Add(field, JToken.FromObject(null));
                        }
                        else
                        {
                            prop.Value = JToken.FromObject(null);
                        }
                    }
                    break;
                case "reference":
                case "ref":
                    {
                        if (!jsons.ContainsKey(value))
                        {
                            jsons.Add(value, new JObject());
                        }
                        var objRef = jsons[value];
                        if (prop == null)
                        {
                            obj.Add(field, objRef);
                        }
                        else
                        {
                            prop.Value = objRef;
                        }
                    }
                    break;

            }
        }

        private static void JsonUnset(this Dictionary<string, JObject> jsons, List<string> parameters)
        {
            var key = parameters[0];
            var field = parameters[1];
            var type = parameters[2].ToLower();
            var value = parameters[3];
            JObject obj = null;
            if (!jsons.ContainsKey(key))
            {
                jsons.Add(key, new JObject());
            }
            obj = jsons[key];
            var prop = obj.Property(field);
            if(prop != null)
            {
                obj.Remove(field);
            }
        }

        private static void JsonPush(this Dictionary<string, JObject> jsons, List<string> parameters)
        {
            var key = parameters[0];
            var field = parameters[1];
            var type = parameters[2].ToLower();
            var value = parameters[3];
            JObject obj = null;
            if (!jsons.ContainsKey(key))
            {
                jsons.Add(key, new JObject());
            }
            obj = jsons[key];
            var prop = obj.Property(field);
            switch (type)
            {
                case "string":
                case "str":
                    {
                        if (prop == null)
                        {
                            obj.Add(field, JToken.FromObject(new string[] { value }));
                        }
                        else
                        {
                            if(prop.Value.Type != JTokenType.Array)
                            {
                                prop.Value = JToken.FromObject(new string[] { value });
                            }
                            else
                            {
                                var arr = prop.Value as JArray;
                                arr.Add(JToken.FromObject(value));
                            }
                        }
                    }
                    break;
                case "integer":
                case "int":
                    {
                        int d = 0;
                        int.TryParse(value, out d);
                        if (prop == null)
                        {
                            obj.Add(field, JToken.FromObject(new int[] { d }));
                        }
                        else
                        {
                            if (prop.Value.Type != JTokenType.Array)
                            {
                                prop.Value = JToken.FromObject(new int[] { d });
                            }
                            else
                            {
                                var arr = prop.Value as JArray;
                                arr.Add(JToken.FromObject(d));
                            }
                        }
                    }
                    break;
                case "float":
                case "double":
                case "dbl":
                    {
                        double d = 0d;
                        double.TryParse(value, out d);
                        if (prop == null)
                        {
                            obj.Add(field, JToken.FromObject(new double[] { d }));
                        }
                        else
                        {
                            if (prop.Value.Type != JTokenType.Array)
                            {
                                prop.Value = JToken.FromObject(new double[] { d });
                            }
                            else
                            {
                                var arr = prop.Value as JArray;
                                arr.Add(JToken.FromObject(d));
                            }
                        }
                    }
                    break;
                case "reference":
                case "ref":
                    {
                        if (!jsons.ContainsKey(value))
                        {
                            jsons.Add(value, new JObject());
                        }
                        var objRef = jsons[value];
                        if (prop == null)
                        {
                            obj.Add(field, new JArray(objRef));
                        }
                        else
                        {
                            if (prop.Value.Type != JTokenType.Array)
                            {
                                prop.Value = new JArray(objRef);
                            }
                            else
                            {
                                var arr = prop.Value as JArray;
                                arr.Add(objRef);
                            }
                        }
                    }
                    break;

            }
        }

        private static void JsonAdd(this Dictionary<string, JObject> jsons, List<string> parameters)
        {
            var key = parameters[0];
            var field = parameters[1];
            var type = parameters[2].ToLower();
            var value = parameters[3];
            JObject obj = null;
            if (!jsons.ContainsKey(key))
            {
                jsons.Add(key, new JObject());
            }
            obj = jsons[key];
            var prop = obj.Property(field);
            switch (type)
            {
                case "string":
                case "str":
                    {
                        if (prop == null)
                        {
                            obj.Add(field, JToken.FromObject(value));
                        }
                        else
                        {
                            prop.Value =  JToken.FromObject(prop.Value.Value<string>() + value);
                        }
                    }
                    break;
                case "integer":
                case "int":
                    {
                        int d = 0;
                        int.TryParse(value, out d);
                        if (prop == null)
                        {
                            obj.Add(field, JToken.FromObject(d));
                        }
                        else
                        {
                            prop.Value = JToken.FromObject(prop.Value.Value<int>() + d);
                        }
                    }
                    break;
                case "float":
                case "double":
                case "dbl":
                    {
                        double d = 0d;
                        double.TryParse(value, out d);
                        if (prop == null)
                        {
                            obj.Add(field, JToken.FromObject(d));
                        }
                        else
                        {
                            prop.Value = JToken.FromObject(prop.Value.Value<double>() + d);
                        }
                    }
                    break;
            }
        }

        private static void JsonAs(this Dictionary<string, JObject> jsons, List<string> parameters)
        {
            var key = parameters[0];
            var asKey = parameters[1];
            JObject obj = null;
            if (!jsons.ContainsKey(key))
            {
                jsons.Add(key, new JObject());
            }
            obj = jsons[key];
            if (jsons.ContainsKey(asKey))
            {
                jsons[asKey] = obj;
            }
            else
            {
                jsons.Add(asKey, obj);
            }
        }

        private static void JsonDelete(this Dictionary<string, JObject> jsons, List<string> parameters)
        {
            var key = parameters[0];
            if (jsons.ContainsKey(key)) jsons.Remove(key);
        }

        private static void JsonDeleteWhere(this Dictionary<string, JObject> jsons, List<string> parameters)
        {
            var pattern = parameters[0];
            foreach(var key in jsons.Keys.ToArray())
            {
                if (Regex.IsMatch(key, pattern)) jsons.Remove(key);
            }
        }

        private static void JsonSave(this Dictionary<string, JObject> jsons, List<string> parameters, IComponentContext componentContext)
        {
            var mode = parameters[0].ToLower();
            var path = parameters[1].ToLower();
            
            if (path.StartsWith("."))
            {
                path = AppContext.BaseDirectory + "/" + path;
            }

            string pattern = null;

            if (parameters.Count > 2)
            {
                pattern = parameters[2];
            }

            string jsonString = "";

            switch (mode)
            {
                case "one":
                    {
                        if(pattern != null && jsons.ContainsKey(pattern))
                        {
                            jsonString = JsonConvert.SerializeObject(jsons[pattern]);
                        }
                    }
                    break;
                case "array":
                    {
                        List<JObject> list;
                        if(pattern != null)
                        {
                            list = jsons
                                .Where(kvp => Regex.IsMatch(kvp.Key, pattern))
                                .Select(kvp => kvp.Value)
                                .ToList();
                        }
                        else
                        {
                            list = jsons.Values.ToList();
                        }
                        jsonString = JsonConvert.SerializeObject(list);
                    }
                    break;
                case "map":
                    {
                        Dictionary<string, JObject> dict;
                        if (pattern != null)
                        {
                            dict = jsons
                                .Where(kvp => Regex.IsMatch(kvp.Key, pattern))
                                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                        }
                        else
                        {
                            dict = jsons.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                        }
                        jsonString = JsonConvert.SerializeObject(dict);
                    }
                    break;
            }

            path.SaveString(jsonString, componentContext);
        }

        private static void JobNew(this Dictionary<string, JObject> jsons, List<string> parameters)
        {
            var key = parameters[0];
            var script = parameters[1];
            var job = parameters[2];
            var ttl = 168;
            int.TryParse(parameters[3], out ttl);
            var shouldSchedule = "false";

            if (parameters.Count > 4)
            {
                shouldSchedule = parameters[4];
            }

            var jObject = JObject.FromObject(new ScriptJob()
            {
                Script = script,
                Job = job,
                Attempts = 0,
                LastSchedule = DateTime.UtcNow.AddHours(-ttl - 1),
                TTL = ttl,
                Payload = null,
                ShouldSchedule = bool.Parse(shouldSchedule),
                State = ScriptJobStateEnum.Runnable,
            });
            if (jsons.ContainsKey(key))
            {
                jsons[key] = jObject;
            }
            else
            {
                jsons.Add(key, jObject);
            }
        }

        private static void JobUpdate(this Dictionary<string, JObject> jsons, List<string> parameters, IComponentContext componentContext)
        {
            var key = parameters[0];
            var jobJson = jsons[key];
            try
            {
                var job = jobJson.ToObject<ScriptJob>(new JsonSerializer()
                {
                    Converters = { new StringEnumConverter() }
                });
                var scheduler = componentContext.Resolve<ScriptJobScheduler>();
                scheduler.CreateScriptJob(job).Wait();
            }
            catch (Exception ex)
            {
                throw new ScrapingException("Failed to Update ScriptJob", ex);
            }
        }

        public static string WindowScrollTo(this IWebDriver driver, string x, string y)
        {
            IJavaScriptExecutor js = driver as IJavaScriptExecutor;
            return js.ExecuteScript($"window.scrollTo({x}, {y});", new object[] { }) as string;
        }

        public static string ScrollIntoView(this IWebElement element, IWebDriver driver)
        {
            IJavaScriptExecutor js = driver as IJavaScriptExecutor;
            return js.ExecuteScript("arguments[0].scrollIntoView();", new object[] { element }) as string;
        }

        public static string GetInnerHtml(this IWebElement element, IWebDriver driver)
        {
            IJavaScriptExecutor js = driver as IJavaScriptExecutor;
            return js.ExecuteScript("return arguments[0].innerHTML;", new object[] { element }) as string;
        }

        public static string GetOuterHtml(this IWebElement element, IWebDriver driver)
        {
            IJavaScriptExecutor js = driver as IJavaScriptExecutor;
            return js.ExecuteScript("return arguments[0].outerHTML;", new object[] { element }) as string;
        }

        public static string ExecuteJs(this IWebDriver driver, string javascript, object[] parameters)
        {
            IJavaScriptExecutor js = driver as IJavaScriptExecutor;
            return js.ExecuteScript(javascript, parameters) as string;
        }

        public static void RunThen(
            this BrowserOperation operation,
            IEnumerable<IWebElement> elements,
            Dictionary<string, string> data,
            Dictionary<string, Action<IEnumerable<IWebElement>>> jobs,
            Dictionary<string, List<IWebElement>> references,
            Dictionary<string, JObject> jsons,
            Dictionary<string, BrowserOperation> functions,
            IWebDriver driver,
            int level,
            IComponentContext componentContext)
        {
            if (operation.Then != null)
            {
                if (operation.Driver)
                {
                    foreach (var unit in operation.Then)
                    {
                        unit.RunOperation(driver, data, jobs, references, jsons, functions, level + 1, componentContext);
                    }
                }
                else if (operation.Batch)
                {
                    foreach (var unit in operation.Then)
                    {
                        unit.RunOperation(elements, data, jobs, references, jsons, functions, driver, level + 1, componentContext);
                    }
                }
                else
                {
                    foreach (var el in elements)
                    {
                        operation.Then.RunOperation(el, data, jobs, references, jsons, functions, driver, level + 1, componentContext);
                    }
                }
            }
        }

        public static void RunDriverThen(
            this BrowserOperation operation,
            IWebDriver driver,
            Dictionary<string, string> data,
            Dictionary<string, Action<IEnumerable<IWebElement>>> jobs,
            Dictionary<string, List<IWebElement>> references,
            Dictionary<string, JObject> jsons,
            Dictionary<string, BrowserOperation> functions,
            int level,
            IComponentContext componentContext)
        {
            if (operation.Then != null)
            {
                foreach (var unit in operation.Then)
                {
                    unit.RunOperation(driver, data, jobs, references, jsons, functions, level + 1, componentContext);
                }
            }
        }

        /// <summary>
        /// run operation in per element mode
        /// </summary>
        /// <param name="operations"></param>
        /// <param name="element"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        public static IEnumerable<IWebElement> RunOperation(
            this IEnumerable<BrowserOperation> operations, 
            IWebElement element, 
            Dictionary<string, string> data,
            Dictionary<string, Action<IEnumerable<IWebElement>>> jobs,
            Dictionary<string, List<IWebElement>> references,
            Dictionary<string, JObject> jsons,
            Dictionary<string, BrowserOperation> functions,
            IWebDriver driver,
            int level,
            IComponentContext componentContext)
        {
            HashSet<IWebElement> results = new HashSet<IWebElement>();
            foreach(var operation in operations)
            {
               var captured = operation.RunOperation(element, data, jobs, references, jsons, functions, driver, level, componentContext);
                if (captured != null) foreach (var el in captured) results.Add(el);
            }
            return results;
        }

        /// <summary>
        /// run operations in batch mode
        /// </summary>
        /// <param name="operations"></param>
        /// <param name="elements"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        public static IEnumerable<IWebElement> RunOperation(
            this BrowserOperation operation, 
            IEnumerable<IWebElement> elements, 
            Dictionary<string, string> data,
            Dictionary<string, Action<IEnumerable<IWebElement>>> jobs,
            Dictionary<string, List<IWebElement>> references,
            Dictionary<string, JObject> jsons,
            Dictionary<string, BrowserOperation> functions,
            IWebDriver driver,
            int level,
            IComponentContext componentContext)
        {
            HashSet<IWebElement> results = new HashSet<IWebElement>();
            foreach (var element in elements)
            {
                var captured = operation.RunOperation(element, data, jobs, references, jsons, functions, driver, level, componentContext);
                if (captured != null) foreach (var el in captured) results.Add(el);
            }
            return results; 
        }

        public static List<string> GetParameters(this BrowserOperation operation, Dictionary<string, string> data)
        {
            if (operation.Parameters == null) return new List<string>();
            return operation.Parameters.Select(parameter => parameter.ApplyTemplateData(data)).ToList();
        }

        private static Regex replaceTemplateKey = new Regex("<#@([^@]+)@#>");
        private static Regex replaceSystemKey = new Regex(@"<#@(\w+)\(([\w-\.,;#@%\$]+)\)@#>");

        private static string ApplyTemplateData(this string value, Dictionary<string, string> data)
        {
            value = replaceSystemKey
                .Replace(value, (match) =>
            {
                var method = match.Groups[1].Value;
                switch (method.ToLower())
                {
                    case "date":
                        return DateTime.UtcNow.ToString(match.Groups[2].Value);
                }
                return "";
            });
            value = replaceTemplateKey
                .Replace(value, (match) =>
            {
                var key = match.Groups[1].Value;
                if (data.ContainsKey(key)) return data[key];
                return "";
            });
            return value;
        }
    }
}
