using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
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
        public static void RunOperations(this IEnumerable<BrowserOperation> operations, 
            IWebDriver driver, 
            IEnumerable<IWebElement> elements, 
            Dictionary<string, string> data,
            Dictionary<string, Action<IEnumerable<IWebElement>>> jobs)
        {
            if(elements == null)
            {
                foreach(var operation in operations)
                {
                    
                }
            }
            else
            {

            }
        }

        public static IEnumerable<IWebElement> RunOperation(
            this BrowserOperation operation, 
            IWebDriver driver, 
            Dictionary<string, string> data,
            Dictionary<string, Action<IEnumerable<IWebElement>>> jobs,
            Dictionary<string, List<IWebElement>> references,
            Dictionary<string, JObject> jsons)
        {
            List<IWebElement> results = new List<IWebElement>();
            var parameters = operation.GetParameters(data);
            bool shouldRunThen = true;
            switch (operation.Action)
            {
                case ActionTypeEnum.GoTo:
                    {
                        driver.Url = parameters[0];
                        driver.Navigate();
                    }
                    break;
                case ActionTypeEnum.Wait:
                    {
                        int seconds = 3;
                        if (parameters.Count >= 1)
                        {
                            int.TryParse(parameters[0], out seconds);
                        }
                        if(parameters.Count >= 4)
                        {
                            var condition = parameters[1].ToLower();
                            var selectorType = parameters[2].ToLower();
                            var selector = parameters[3];
                            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(seconds));
                            By by = By.XPath(".");
                            IWebElement found;
                            switch (selectorType)
                            {
                                case "css":
                                    by = By.CssSelector(selector);
                                    break;
                                case "id":
                                    by = By.Id(selector);
                                    break;
                                case "xpath":
                                    by = By.XPath(selector);
                                    break;
                                case "class":
                                    by = By.ClassName(selector);
                                    break;
                            }
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
                    break;
                case ActionTypeEnum.By:
                    {
                        if(parameters.Count >= 2)
                        {
                            var selectorType = parameters[0].ToLower();
                            var selector = parameters[1];
                            By by = By.XPath(".");
                            switch (selectorType)
                            {
                                case "css":
                                    by = By.CssSelector(selector);
                                    break;
                                case "id":
                                    by = By.Id(selector);
                                    break;
                                case "xpath":
                                    by = By.XPath(selector);
                                    break;
                                case "class":
                                    by = By.ClassName(selector);
                                    break;
                            }
                            if (operation.Multiple)
                            {
                                var elements = driver.FindElements(by);
                                results.AddRange(elements);
                            }
                            else
                            {
                                var element = driver.FindElement(by);
                                results.Add(element);
                            }
                        }
                    }
                    break;
                case ActionTypeEnum.LoopWhen:
                    {
                        List<IWebElement> looping = new List<IWebElement>();
                        if (parameters.Count >= 5)
                        {
                            int count = 1;
                            if (!int.TryParse(parameters[0], out count))
                            {
                                throw new ScrapingException($"The 1st parameter of {nameof(ActionTypeEnum.LoopWhen)} must be a number. It currently is {parameters[0]}");
                            }
                            var selectorType = parameters[1].ToLower();
                            var selector = parameters[2];
                            var condition = parameters[3];
                            var number = 0;
                            if (!int.TryParse(parameters[4], out number))
                            {
                                throw new ScrapingException($"The 54th parameter of {nameof(ActionTypeEnum.LoopWhen)} must be a number. It currently is {parameters[4]}");
                            }
                            string textFilter = "none";
                            string pattern = null;
                            string attr = null;
                            if (parameters.Count >= 7)
                            {
                                textFilter = parameters[5].ToLower();
                                pattern = parameters[6];
                                if (textFilter == "attr")
                                {
                                    if (parameters.Count >= 8)
                                    {
                                        attr = parameters[7];
                                    }
                                    else
                                    {
                                        throw new ScrapingException($"The 8th parameter of {nameof(ActionTypeEnum.LoopWhen)} [attribute key] is missing.");
                                    }
                                }
                            }
                            By by = By.XPath(".");
                            switch (selectorType)
                            {
                                case "css":
                                    by = By.CssSelector(selector);
                                    break;
                                case "id":
                                    by = By.Id(selector);
                                    break;
                                case "xpath":
                                    by = By.XPath(selector);
                                    break;
                                case "class":
                                    by = By.ClassName(selector);
                                    break;
                            }
                            bool shouldLoop = true;
                            while (shouldLoop && count > 0)
                            {
                                var found = driver.FindElements(by).ToList();
                                switch (textFilter)
                                {
                                    case "attr":
                                        found = found.Where(element => Regex.IsMatch(element.GetAttribute(attr), pattern)).ToList();
                                        break;
                                    case "inner":
                                        found = found.Where(element => Regex.IsMatch(element.GetInnerHtml(driver), pattern)).ToList();
                                        break;
                                    case "outer":
                                        found = found.Where(element => Regex.IsMatch(element.GetOuterHtml(driver), pattern)).ToList();
                                        break;
                                    default:
                                        break;
                                }
                                switch (condition)
                                {
                                    case "=":
                                    case "==":
                                        {
                                            shouldLoop = found.Count == number;
                                        }
                                        break;
                                    case "!=":
                                        {
                                            shouldLoop = found.Count != number;
                                        }
                                        break;
                                    case ">":
                                        {
                                            shouldLoop = found.Count > number;
                                        }
                                        break;
                                    case "<":
                                        {
                                            shouldLoop = found.Count < number;
                                        }
                                        break;
                                    case ">=":
                                        {
                                            shouldLoop = found.Count >= number;
                                        }
                                        break;
                                    case "<=":
                                        {
                                            shouldLoop = found.Count <= number;
                                        }
                                        break;
                                }
                                if (shouldLoop)
                                {
                                    if (found.Any())
                                    {
                                        operation.RunThen(found, data, jobs, references, jsons, driver);
                                    }
                                    else
                                    {
                                        operation.RunDriveThen(driver, data, jobs, references, jsons);
                                    }
                                }
                                count--;
                            }
                        }
                        else
                        {
                            throw new ScrapingException($"{nameof(ActionTypeEnum.LoopWhen)} must have 5 parameters. It currently has {parameters.Count}.");
                        }
                        shouldRunThen = false;
                    }
                    break;
                case ActionTypeEnum.LoopUntil:
                    {
                        List<IWebElement> looping = new List<IWebElement>();
                        if (parameters.Count >= 5)
                        {
                            int count = 1;
                            if (!int.TryParse(parameters[0], out count))
                            {
                                throw new ScrapingException($"The 1st parameter of {nameof(ActionTypeEnum.LoopWhen)} must be a number. It currently is {parameters[0]}");
                            }
                            var selectorType = parameters[1].ToLower();
                            var selector = parameters[2];
                            var condition = parameters[3];
                            var number = 0;
                            if (!int.TryParse(parameters[4], out number))
                            {
                                throw new ScrapingException($"The 5th parameter of {nameof(ActionTypeEnum.LoopWhen)} must be a number. It currently is {parameters[4]}");
                            }
                            string textFilter = "none";
                            string pattern = null;
                            string attr = null;
                            if (parameters.Count >= 7)
                            {
                                textFilter = parameters[5].ToLower();
                                pattern = parameters[6];
                                if (textFilter == "attr")
                                {
                                    if (parameters.Count >= 8)
                                    {
                                        attr = parameters[7];
                                    }
                                    else
                                    {
                                        throw new ScrapingException($"The 8th parameter of {nameof(ActionTypeEnum.LoopWhen)} [attribute key] is missing.");
                                    }
                                }
                            }

                            By by = By.XPath(".");
                            switch (selectorType)
                            {
                                case "css":
                                    by = By.CssSelector(selector);
                                    break;
                                case "id":
                                    by = By.Id(selector);
                                    break;
                                case "xpath":
                                    by = By.XPath(selector);
                                    break;
                                case "class":
                                    by = By.ClassName(selector);
                                    break;
                            }
                            bool shouldNotLoop = true;
                            do
                            {
                                var found = driver.FindElements(by).ToList();
                                switch (textFilter)
                                {
                                    case "attr":
                                        found = found.Where(element => Regex.IsMatch(element.GetAttribute(attr), pattern)).ToList();
                                        break;
                                    case "inner":
                                        found = found.Where(element => Regex.IsMatch(element.GetInnerHtml(driver), pattern)).ToList();
                                        break;
                                    case "outer":
                                        found = found.Where(element => Regex.IsMatch(element.GetOuterHtml(driver), pattern)).ToList();
                                        break;
                                    default:
                                        break;
                                }
                                switch (condition)
                                {
                                    case "=":
                                    case "==":
                                        {
                                            shouldNotLoop = found.Count == number;
                                        }
                                        break;
                                    case "!=":
                                        {
                                            shouldNotLoop = found.Count != number;
                                        }
                                        break;
                                    case ">":
                                        {
                                            shouldNotLoop = found.Count > number;
                                        }
                                        break;
                                    case "<":
                                        {
                                            shouldNotLoop = found.Count < number;
                                        }
                                        break;
                                    case ">=":
                                        {
                                            shouldNotLoop = found.Count >= number;
                                        }
                                        break;
                                    case "<=":
                                        {
                                            shouldNotLoop = found.Count <= number;
                                        }
                                        break;
                                }
                                if (!shouldNotLoop)
                                {
                                    if (found.Any())
                                    {
                                        operation.RunThen(found, data, jobs, references, jsons, driver);
                                    }
                                    else
                                    {
                                        operation.RunDriveThen(driver, data, jobs, references, jsons);
                                    }
                                }
                                count--;
                            } while (!shouldNotLoop && count > 0);
                        }
                        else
                        {
                            throw new ScrapingException($"{nameof(ActionTypeEnum.LoopUntil)} must have 5 parameters. It currently has {parameters.Count}.");
                        }
                        shouldRunThen = false;
                    }
                    break;
                case ActionTypeEnum.Click:
                    throw new ScrapingException($"{nameof(ActionTypeEnum.Click)} is not avaliable on {nameof(IWebDriver)}.");
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
                case ActionTypeEnum.PutAttr:
                    throw new ScrapingException($"{nameof(ActionTypeEnum.PutAttr)} is not avaliable on {nameof(IWebDriver)}.");
                case ActionTypeEnum.PutInner:
                    throw new ScrapingException($"{nameof(ActionTypeEnum.PutInner)} is not avaliable on {nameof(IWebDriver)}.");
                case ActionTypeEnum.PutOuter:
                    throw new ScrapingException($"{nameof(ActionTypeEnum.PutOuter)} is not avaliable on {nameof(IWebDriver)}.");
                case ActionTypeEnum.CollectAttr:
                    throw new ScrapingException($"{nameof(ActionTypeEnum.CollectAttr)} is not avaliable on {nameof(IWebDriver)}.");
                case ActionTypeEnum.CollectInner:
                    throw new ScrapingException($"{nameof(ActionTypeEnum.CollectInner)} is not avaliable on {nameof(IWebDriver)}.");
                case ActionTypeEnum.CollectOuter:
                    throw new ScrapingException($"{nameof(ActionTypeEnum.CollectOuter)} is not avaliable on {nameof(IWebDriver)}.");
                case ActionTypeEnum.SplitOne:
                    shouldRunThen = data.SplitOne(parameters);
                    break;
                case ActionTypeEnum.LoopSplitOne:
                    {
                        while (data.SplitOne(parameters))
                        {
                            operation.RunDriveThen(driver, data, jobs, references, jsons);
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
                case ActionTypeEnum.JsonDelete:
                    jsons.JsonDelete(parameters);
                    break;
                case ActionTypeEnum.JsonDeleteWhere:
                    jsons.JsonDeleteWhere(parameters);
                    break;
                case ActionTypeEnum.JsonSave:
                    jsons.JsonSave(parameters);
                    break;
                case ActionTypeEnum.Log:
                    parameters.Log();
                    break;
                case ActionTypeEnum.LogJson:
                    parameters.LogJson(jsons);
                    break;
            }
            if(shouldRunThen) operation.RunThen(results, data, jobs, references, jsons, driver);
            return results;
        }

        public static List<IWebElement> RunOperation(
            this BrowserOperation operation, 
            IWebElement parent, 
            Dictionary<string, string> data,
            Dictionary<string, Action<IEnumerable<IWebElement>>> jobs,
            Dictionary<string, List<IWebElement>> references,
            Dictionary<string, JObject> jsons,
            IWebDriver driver)
        {

            List<IWebElement> results = new List<IWebElement>();
            var parameters = operation.GetParameters(data);
            bool shouldRunThen = true;
            switch (operation.Action)
            {
                case ActionTypeEnum.GoTo:
                    throw new ScrapingException($"{nameof(ActionTypeEnum.GoTo)} is not avaliable on {nameof(IWebElement)}.");
                case ActionTypeEnum.Wait:
                    {
                        int seconds = 3;
                        if (parameters.Count >= 1)
                        {
                            int.TryParse(parameters[0], out seconds);
                        }
                        if (parameters.Count >= 4)
                        {
                            var condition = parameters[1].ToLower();
                            var selectorType = parameters[2].ToLower();
                            var selector = parameters[3];
                            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(seconds));
                            By by = By.XPath(".");
                            IWebElement found;
                            switch (selectorType)
                            {
                                case "css":
                                    by = By.CssSelector(selector);
                                    break;
                                case "id":
                                    by = By.Id(selector);
                                    break;
                                case "xpath":
                                    by = By.XPath(selector);
                                    break;
                                case "class":
                                    by = By.ClassName(selector);
                                    break;
                            }
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
                    break;
                case ActionTypeEnum.LoopWhen:
                    {
                        List<IWebElement> looping = new List<IWebElement>();
                        if (parameters.Count >= 5)
                        {
                            int count = 1;
                            if (!int.TryParse(parameters[0], out count))
                            {
                                throw new ScrapingException($"The 1st parameter of {nameof(ActionTypeEnum.LoopWhen)} must be a number. It currently is {parameters[0]}");
                            }
                            var selectorType = parameters[1].ToLower();
                            var selector = parameters[2];
                            var condition = parameters[3];
                            var number = 0;
                            if (!int.TryParse(parameters[4], out number))
                            {
                                throw new ScrapingException($"The 54th parameter of {nameof(ActionTypeEnum.LoopWhen)} must be a number. It currently is {parameters[4]}");
                            }
                            string textFilter = "none";
                            string pattern = null;
                            string attr = null;
                            if (parameters.Count >= 7)
                            {
                                textFilter = parameters[5].ToLower();
                                pattern = parameters[6];
                                if (textFilter == "attr")
                                {
                                    if (parameters.Count >= 8)
                                    {
                                        attr = parameters[7];
                                    }
                                    else
                                    {
                                        throw new ScrapingException($"The 8th parameter of {nameof(ActionTypeEnum.LoopWhen)} [attribute key] is missing.");
                                    }
                                }
                            }
                            By by = By.XPath(".");
                            switch (selectorType)
                            {
                                case "css":
                                    by = By.CssSelector(selector);
                                    break;
                                case "id":
                                    by = By.Id(selector);
                                    break;
                                case "xpath":
                                    by = By.XPath(selector);
                                    break;
                                case "class":
                                    by = By.ClassName(selector);
                                    break;
                            }
                            bool shouldLoop = true;
                            while (shouldLoop && count > 0)
                            {
                                var found = parent.FindElements(by).ToList();
                                switch (textFilter)
                                {
                                    case "attr":
                                        found = found.Where(element => Regex.IsMatch(element.GetAttribute(attr), pattern)).ToList();
                                        break;
                                    case "inner":
                                        found = found.Where(element => Regex.IsMatch(element.GetInnerHtml(driver), pattern)).ToList();
                                        break;
                                    case "outer":
                                        found = found.Where(element => Regex.IsMatch(element.GetOuterHtml(driver), pattern)).ToList();
                                        break;
                                    default:
                                        break;
                                }
                                switch (condition)
                                {
                                    case "=":
                                    case "==":
                                        {
                                            shouldLoop = found.Count == number;
                                        }
                                        break;
                                    case "!=":
                                        {
                                            shouldLoop = found.Count != number;
                                        }
                                        break;
                                    case ">":
                                        {
                                            shouldLoop = found.Count > number;
                                        }
                                        break;
                                    case "<":
                                        {
                                            shouldLoop = found.Count < number;
                                        }
                                        break;
                                    case ">=":
                                        {
                                            shouldLoop = found.Count >= number;
                                        }
                                        break;
                                    case "<=":
                                        {
                                            shouldLoop = found.Count <= number;
                                        }
                                        break;
                                }
                                if (shouldLoop)
                                {
                                    if (found.Any())
                                    {
                                        operation.RunThen(found, data, jobs, references, jsons, driver);
                                    }
                                    else
                                    {
                                        operation.RunDriveThen(driver, data, jobs, references, jsons);
                                    }
                                }
                                count--;
                            }
                        }
                        else
                        {
                            throw new ScrapingException($"{nameof(ActionTypeEnum.LoopWhen)} must have 5 parameters. It currently has {parameters.Count}.");
                        }
                        shouldRunThen = false;
                    }
                    break;
                case ActionTypeEnum.LoopUntil:
                    {
                        List<IWebElement> looping = new List<IWebElement>();
                        if (parameters.Count >= 5)
                        {
                            int count = 1;
                            if (!int.TryParse(parameters[0], out count))
                            {
                                throw new ScrapingException($"The 1st parameter of {nameof(ActionTypeEnum.LoopWhen)} must be a number. It currently is {parameters[0]}");
                            }
                            var selectorType = parameters[1].ToLower();
                            var selector = parameters[2];
                            var condition = parameters[3];
                            var number = 0;
                            if (!int.TryParse(parameters[4], out number))
                            {
                                throw new ScrapingException($"The 5th parameter of {nameof(ActionTypeEnum.LoopWhen)} must be a number. It currently is {parameters[4]}");
                            }
                            string textFilter = "none";
                            string pattern = null;
                            string attr = null;
                            if(parameters.Count >= 7)
                            {
                                textFilter = parameters[5].ToLower();
                                pattern = parameters[6];
                                if (textFilter == "attr")
                                {
                                    if(parameters.Count >= 8)
                                    {
                                        attr = parameters[7];
                                    }
                                    else
                                    {
                                        throw new ScrapingException($"The 8th parameter of {nameof(ActionTypeEnum.LoopWhen)} [attribute key] is missing.");
                                    }
                                }
                            }

                            By by = By.XPath(".");
                            switch (selectorType)
                            {
                                case "css":
                                    by = By.CssSelector(selector);
                                    break;
                                case "id":
                                    by = By.Id(selector);
                                    break;
                                case "xpath":
                                    by = By.XPath(selector);
                                    break;
                                case "class":
                                    by = By.ClassName(selector);
                                    break;
                            }
                            bool shouldNotLoop = true;
                            do
                            {
                                var found = parent.FindElements(by).ToList();
                                switch (textFilter)
                                {
                                    case "attr":
                                        found = found.Where(element => Regex.IsMatch(element.GetAttribute(attr), pattern)).ToList();
                                        break;
                                    case "inner":
                                        found = found.Where(element => Regex.IsMatch(element.GetInnerHtml(driver), pattern)).ToList();
                                        break;
                                    case "outer":
                                        found = found.Where(element => Regex.IsMatch(element.GetOuterHtml(driver), pattern)).ToList();
                                        break;
                                    default:
                                        break;
                                }
                                switch (condition)
                                {
                                    case "=":
                                    case "==":
                                        {
                                            shouldNotLoop = found.Count == number;
                                        }
                                        break;
                                    case "!=":
                                        {
                                            shouldNotLoop = found.Count != number;
                                        }
                                        break;
                                    case ">":
                                        {
                                            shouldNotLoop = found.Count > number;
                                        }
                                        break;
                                    case "<":
                                        {
                                            shouldNotLoop = found.Count < number;
                                        }
                                        break;
                                    case ">=":
                                        {
                                            shouldNotLoop = found.Count >= number;
                                        }
                                        break;
                                    case "<=":
                                        {
                                            shouldNotLoop = found.Count <= number;
                                        }
                                        break;
                                }
                                if (!shouldNotLoop)
                                {
                                    if (found.Any())
                                    {
                                        operation.RunThen(found, data, jobs, references, jsons, driver);
                                    }
                                    else
                                    {
                                        operation.RunDriveThen(driver, data, jobs, references, jsons);
                                    }
                                }
                                count--;
                            } while (!shouldNotLoop && count > 0);
                        }
                        else
                        {
                            throw new ScrapingException($"{nameof(ActionTypeEnum.LoopUntil)} must have 5 parameters. It currently has {parameters.Count}.");
                        }
                        shouldRunThen = false;
                    }
                    break;
                case ActionTypeEnum.By:
                    {
                        if (parameters.Count >= 2)
                        {
                            var selectorType = parameters[0].ToLower();
                            var selector = parameters[1];
                            By by = By.XPath(".");
                            switch (selectorType)
                            {
                                case "css":
                                    by = By.CssSelector(selector);
                                    break;
                                case "id":
                                    by = By.Id(selector);
                                    break;
                                case "xpath":
                                    by = By.XPath(selector);
                                    break;
                                case "class":
                                    by = By.ClassName(selector);
                                    break;
                            }
                            if(parameters.Count >=3 && parameters[2].ToLower() == "root")
                            {
                                if (operation.Multiple)
                                {
                                    var elements = driver.FindElements(by);
                                    results.AddRange(elements);
                                }
                                else
                                {
                                    var element = driver.FindElement(by);
                                    results.Add(element);
                                }
                            }
                            else
                            {
                                if (operation.Multiple)
                                {
                                    var elements = parent.FindElements(by);
                                    results.AddRange(elements);
                                }
                                else
                                {
                                    var element = parent.FindElement(by);
                                    results.Add(element);
                                }
                            }
                        }
                    }
                    break;
                case ActionTypeEnum.Click:
                    {
                        parent.Click();
                        results.Add(parent);
                    }
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
                        if(attr != null && Regex.IsMatch(attr, parameters[1]))
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
                case ActionTypeEnum.PutAttr:
                    {
                        var key = parameters[0];
                        var attr = parent.GetAttribute(parameters[1]);
                        if(parameters.Count >= 3)
                        {
                            var pattern = parameters[2];
                            var match = Regex.Match(attr, pattern);
                            if (match.Success)
                            {
                                if (data.ContainsKey(key))
                                {
                                    data[key] = match.Value;
                                }
                                else
                                {
                                    data.Add(key, match.Value);
                                }
                            }
                        }
                        else
                        {
                            if (data.ContainsKey(key))
                            {
                                data[key] = attr;
                            }
                            else
                            {
                                data.Add(key, attr);
                            }
                        }
                    }
                    break;
                case ActionTypeEnum.PutInner:
                    {
                        var key = parameters[0];
                        var attr = parent.GetInnerHtml(driver);
                        if (parameters.Count >= 2)
                        {
                            var pattern = parameters[1];
                            var match = Regex.Match(attr, pattern);
                            if (match.Success)
                            {
                                if (data.ContainsKey(key))
                                {
                                    data[key] = match.Value;
                                }
                                else
                                {
                                    data.Add(key, match.Value);
                                }
                            }
                        }
                        else
                        {
                            if (data.ContainsKey(key))
                            {
                                data[key] = attr;
                            }
                            else
                            {
                                data.Add(key, attr);
                            }
                        }
                    }
                    break;
                case ActionTypeEnum.PutOuter:
                    {
                        var key = parameters[0];
                        var attr = parent.GetOuterHtml(driver);
                        if (parameters.Count >= 2)
                        {
                            var pattern = parameters[1];
                            var match = Regex.Match(attr, pattern);
                            if (match.Success)
                            {
                                if (data.ContainsKey(key))
                                {
                                    data[key] = match.Value;
                                }
                                else
                                {
                                    data.Add(key, match.Value);
                                }
                            }
                        }
                        else
                        {
                            if (data.ContainsKey(key))
                            {
                                data[key] = attr;
                            }
                            else
                            {
                                data.Add(key, attr);
                            }
                        }
                    }
                    break;
                case ActionTypeEnum.CollectAttr:
                    {
                        var key = parameters[0];
                        var attr = parent.GetAttribute(parameters[1]);
                        var separator = parameters[2];
                        if (parameters.Count >= 4)
                        {
                            var pattern = parameters[2];
                            var match = Regex.Match(attr, pattern);
                            if (match.Success)
                            {
                                if (data.ContainsKey(key))
                                {
                                    data[key] += separator + match.Value;
                                }
                                else
                                {
                                    data.Add(key, match.Value);
                                }
                            }
                        }
                        else
                        {
                            if (data.ContainsKey(key))
                            {
                                data[key] += separator + attr;
                            }
                            else
                            {
                                data.Add(key, attr);
                            }
                        }
                    }
                    break;
                case ActionTypeEnum.CollectInner:
                    {
                        var key = parameters[0];
                        var attr = parent.GetInnerHtml(driver);
                        var separator = parameters[1];
                        if (parameters.Count >= 3)
                        {
                            var pattern = parameters[2];
                            var match = Regex.Match(attr, pattern);
                            if (match.Success)
                            {
                                if (data.ContainsKey(key))
                                {
                                    data[key] += separator + match.Value;
                                }
                                else
                                {
                                    data.Add(key, match.Value);
                                }
                            }
                        }
                        else
                        {
                            if (data.ContainsKey(key))
                            {
                                data[key] += separator + attr;
                            }
                            else
                            {
                                data.Add(key, attr);
                            }
                        }
                    }
                    break;
                case ActionTypeEnum.CollectOuter:
                    {
                        var key = parameters[0];
                        var attr = parent.GetOuterHtml(driver);
                        var separator = parameters[1];
                        if (parameters.Count >= 3)
                        {
                            var pattern = parameters[2];
                            var match = Regex.Match(attr, pattern);
                            if (match.Success)
                            {
                                if (data.ContainsKey(key))
                                {
                                    data[key] += separator + match.Value;
                                }
                                else
                                {
                                    data.Add(key, match.Value);
                                }
                            }
                        }
                        else
                        {
                            if (data.ContainsKey(key))
                            {
                                data[key] += separator + attr;
                            }
                            else
                            {
                                data.Add(key, attr);
                            }
                        }
                    }
                    break;
                case ActionTypeEnum.SplitOne:
                    shouldRunThen = data.SplitOne(parameters);
                    break;
                case ActionTypeEnum.LoopSplitOne:
                    {
                        while (data.SplitOne(parameters))
                        {
                            operation.RunDriveThen(driver, data, jobs, references, jsons);
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
                case ActionTypeEnum.JsonDelete:
                    jsons.JsonDelete(parameters);
                    break;
                case ActionTypeEnum.JsonDeleteWhere:
                    jsons.JsonDeleteWhere(parameters);
                    break;
                case ActionTypeEnum.JsonSave:
                    jsons.JsonSave(parameters);
                    break;
                case ActionTypeEnum.Log:
                    parameters.Log();
                    break;
                case ActionTypeEnum.LogJson:
                    parameters.LogJson(jsons);
                    break;
            }
            if(shouldRunThen) operation.RunThen(results, data, jobs, references, jsons, driver);
            return results;
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
            foreach(var parameter in parameters)
            {
                Console.WriteLine(parameter);
            }
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

        private static void JsonSave(this Dictionary<string, JObject> jsons, List<string> parameters)
        {
            var mode = parameters[0].ToLower();
            var targetType = parameters[1].ToLower();
            var path = parameters[2].ToLower();
            if (path.StartsWith("."))
            {
                path = AppContext.BaseDirectory + "/" + path;
            }
            string pattern = null;
            if (parameters.Count >= 4)
            {
                pattern = parameters[3];
            }
            string jsonString = "";
            switch (mode)
            {
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

            switch (targetType)
            {
                case "file":
                    {
                        File.WriteAllText(path, jsonString);
                    }
                    break;
                case "s3":
                    {

                    }
                    break;
                case "blobstorage":
                    {

                    }
                    break;
            }
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
            IWebDriver driver)
        {
            if (operation.Then != null)
            {
                if (operation.Batch)
                {
                    foreach (var unit in operation.Then)
                    {
                        unit.RunOperation(elements, data, jobs, references, jsons, driver);
                    }
                }
                else
                {
                    foreach (var el in elements)
                    {
                        operation.Then.RunOperation(el, data, jobs, references, jsons, driver);
                    }
                }
            }
        }

        public static void RunDriveThen(
            this BrowserOperation operation,
            IWebDriver driver,
            Dictionary<string, string> data,
            Dictionary<string, Action<IEnumerable<IWebElement>>> jobs,
            Dictionary<string, List<IWebElement>> references,
            Dictionary<string, JObject> jsons)
        {
            if (operation.Then != null)
            {
                foreach (var unit in operation.Then)
                {
                    unit.RunOperation(driver, data, jobs, references, jsons);
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
            IWebDriver driver)
        {
            HashSet<IWebElement> results = new HashSet<IWebElement>();
            foreach(var operation in operations)
            {
               var captured = operation.RunOperation(element, data, jobs, references, jsons, driver);
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
            IWebDriver driver)
        {
            HashSet<IWebElement> results = new HashSet<IWebElement>();
            foreach (var element in elements)
            {
                var captured = operation.RunOperation(element, data, jobs, references, jsons, driver);
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

        private static string ApplyTemplateData(this string value, Dictionary<string, string> data)
        {
            return replaceTemplateKey.Replace(value, (match) =>
            {
                var key = match.Groups[1].Value;
                if (data.ContainsKey(key)) return data[key];
                return "";
            });
        }
    }
}
