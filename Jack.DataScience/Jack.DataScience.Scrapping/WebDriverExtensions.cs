﻿using OpenQA.Selenium;
using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.ObjectModel;
using OpenQA.Selenium.Support.UI;
using System.Threading.Tasks;
using SeleniumExtras.WaitHelpers;
using Newtonsoft.Json;
using System.Diagnostics;

namespace Jack.DataScience.Scrapping
{
    public static class WebDriverExtensions
    {
        public static IWebElement XPath(this IWebDriver driver, string xpath)
        {
            return driver.FindElement(By.XPath(xpath));
        }

        public static IWebElement TryXPath(this IWebDriver driver, string xpath)
        {
            try
            {
                return driver.FindElement(By.XPath(xpath));
            }
            catch(Exception ex)
            {
                return null;
            }
        }

        public static ReadOnlyCollection<IWebElement> XPaths(this IWebDriver driver, string xpath)
        {
            return driver.FindElements(By.XPath(xpath));
        }
        public static IWebElement XPath(this IWebElement element, string xpath)
        {
            return element.FindElement(By.XPath(xpath));
        }

        [DebuggerNonUserCode]
        public static IWebElement TryXPath(this IWebElement element, string xpath)
        {
            try
            {
                return element.FindElement(By.XPath(xpath));
            }
            catch(Exception ex)
            {
                return null;
            }
        }

        public static ReadOnlyCollection<IWebElement> XPaths(this IWebElement element, string xpath)
        {
            return element.FindElements(By.XPath(xpath));
        }

        public static void Wait(this IWebDriver driver, int seconds)
        {
            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(seconds + 5));
            wait.PollingInterval = TimeSpan.FromSeconds(seconds);
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            wait.Until(d => stopwatch.ElapsedMilliseconds >= seconds * 1000);
        }

        public static void WaitUntilExists(this IWebDriver driver, string xpath, int seconds)
        {
            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(seconds));
            By by = By.XPath(xpath);
            wait.Until(ExpectedConditions.ElementExists(by));
        }

        public static IWebElement WaitUntilIsVisible(this IWebDriver driver, string xpath, int seconds)
        {
            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(seconds));
            By by = By.XPath(xpath);
            return wait.Until(ExpectedConditions.ElementIsVisible(by));
        }

        public static IWebElement WaitUntilIsClickable(this IWebDriver driver, string xpath, int seconds)
        {
            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(seconds));
            By by = By.XPath(xpath);
            return wait.Until(ExpectedConditions.ElementToBeClickable(by));
        }

        public static T DeserializeJson<T>(this string json)
        {
            return JsonConvert.DeserializeObject<T>(json);
        }

        public static void HeatBeat(this IWebDriver driver)
        {
            Console.WriteLine("***HeartBeat***");
        }
    }
}
