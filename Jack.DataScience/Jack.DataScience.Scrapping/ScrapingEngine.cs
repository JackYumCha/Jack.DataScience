using Autofac;
using Newtonsoft.Json.Linq;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Jack.DataScience.Scrapping
{
    public class ScrapingEngine
    {
        public const string ChromeDriverPathKey = "CHROME_DRIVER_PATH";
        private IComponentContext componentContext;

        public ScrapingEngine() {}

        public ScrapingEngine(IComponentContext componentContext)
        {
            this.componentContext = componentContext;
            TryResolve();
        }

        public void TryResolve()
        {
            ChromeOptions chromeOptions = null;
            if (componentContext.TryResolve(out chromeOptions)) ChromeOptions = chromeOptions;
            BrowserScripts scripts = null;
            if (componentContext.TryResolve(out scripts)) Scripts = scripts;
        }

        public BrowserScripts Scripts { get; set; }
        public ChromeOptions ChromeOptions { get; set; }
        public Dictionary<string, Action<IEnumerable<IWebElement>>> Jobs { get; set; } = new Dictionary<string, Action<IEnumerable<IWebElement>>>();
        public Dictionary<string, List<IWebElement>> References { get; set; } = new Dictionary<string, List<IWebElement>>();
        public Dictionary<string, JObject> Jsons { get; set; } = new Dictionary<string, JObject>();

        public Dictionary<string, string> RunScript(string key, Dictionary<string,string> dynamicData = null, ChromeOptions options = null)
        {
            if (dynamicData == null) dynamicData = new Dictionary<string, string>();
            if (Scripts == null || Scripts.Scripts == null || !Scripts.Scripts.ContainsKey(key) ) return dynamicData;

            var operations = Scripts.Scripts[key];
           
            string ChromeDriverPath = Environment.GetEnvironmentVariable(ChromeDriverPathKey);

            if(!string.IsNullOrEmpty( ChromeDriverPath)) Environment.SetEnvironmentVariable("PATH", ChromeDriverPath);

            ChromeDriver chromeDriver;

            // overwrite default
            if (options == null && ChromeOptions != null) options = ChromeOptions;

            if(options == null)
            {
                chromeDriver = new ChromeDriver();
            }
            else
            {
                chromeDriver = new ChromeDriver(options);
            }
            
            foreach(var operation in operations)
            {
                operation.RunOperation(chromeDriver, dynamicData, Jobs, References, Jsons);
            }

            Debugger.Break();

            chromeDriver.Dispose();
            return dynamicData;
        }
    }
}
