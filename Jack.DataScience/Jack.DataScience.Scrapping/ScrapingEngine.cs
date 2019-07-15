using Amazon.S3.Model;
using Autofac;
using Jack.DataScience.Storage.AWSS3;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
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
        public Dictionary<string, BrowserOperation> Functions { get; set; } = new Dictionary<string, BrowserOperation>();

        public Dictionary<string, string> RunScript(string key, Dictionary<string,string> dynamicData = null, ChromeOptions options = null)
        {
            Console.WriteLine($"RunScript({key})");

            if (dynamicData == null) dynamicData = new Dictionary<string, string>();
            if (Scripts == null || Scripts.Scripts == null || !Scripts.Scripts.ContainsKey(key) )
            {
                Console.WriteLine($"Script Key={key} does not exist in the Scripts Data.");
                return dynamicData;
            }

            var operations = Scripts.Scripts[key];

            string ChromeDriverPath = "/usr/bin/chromedriver"; // Environment.GetEnvironmentVariable(ChromeDriverPathKey);

            //if(!string.IsNullOrEmpty( ChromeDriverPath)) Environment.SetEnvironmentVariable("PATH", ChromeDriverPath);

            ChromeDriver chromeDriver;

            // overwrite default
            if (options == null && ChromeOptions != null) options = ChromeOptions;

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                Console.WriteLine($"Chrome Driver in Linux: {ChromeDriverPath}");
                if (options == null) options = new ChromeOptions();
                options.AddArgument("--headless");
                options.AddArgument("--disable-gpu");
                options.AddArgument("--no-sandbox");
                options.AddArgument("--disable-dev-shm-usage");
                options.AddArguments("--start-maximized");
                //options.AddArgument("--window-size=1680,1440");
                options.AddArgument("--log-level=0");
                //options.AddArgument("--verbose");
                options.BinaryLocation = ChromeDriverPath;
                // options.BinaryLocation = 
            }

            if (options == null)
            {
                chromeDriver = new ChromeDriver();
            }
            else
            {
                chromeDriver = new ChromeDriver(options);
            }

            try
            {
                foreach (var operation in operations)
                {
                    operation.RunOperation(chromeDriver, dynamicData, Jobs, References, Jsons, Functions, 0, componentContext);
                }
                if (dynamicData.ContainsKey("success"))
                {
                    dynamicData["success"] = "true";
                }
                else
                {
                    dynamicData.Add("success", "true");
                }
            }
            catch(Exception ex)
            {
                do
                {
                    Console.Error.WriteLine($"[Exception] {ex.GetType().FullName}");
                    Console.Error.WriteLine($"[Error Message] {ex.Message}");
                    Console.Error.WriteLine($"[Stack Trace]:\n {ex.StackTrace}");
                    ex = ex.InnerException;
                } while (ex.InnerException != null);
                if (Functions.ContainsKey("error"))
                {
                    Functions["error"].RunDriverThen(chromeDriver, dynamicData, Jobs, References, Jsons, Functions, 0, componentContext);
                    if (dynamicData.ContainsKey("success"))
                    {
                        dynamicData["success"] = "false";
                    }
                    else
                    {
                        dynamicData.Add("success", "false");
                    }
                }
            }

            //Debugger.Break();

            chromeDriver.Dispose();
            return dynamicData;
        }

        public async Task<bool> RunQueueJob()
        {
            var scheduler = componentContext.Resolve<ScriptJobScheduler>();
            var s3 = componentContext.Resolve<AWSS3API>();
            var options = componentContext.Resolve<AWSScrapeJobOptions>();

            ScriptJob job= null;
            ScriptJobMessage message = null;
            S3Object scriptFileObj = null;
            while (job == null)
            {
                message = await scheduler.GetScriptJob();
                if (message == null) return false;

                // run the job
                job = message.Job;
                try
                {
                    if (options.TestMode)
                    {
                        if (options.TestScriptMapping.ContainsKey(job.Script))
                        {
                            var filename = options.TestScriptMapping[job.Script];
                            if (filename.StartsWith(".")) filename = $"{AppContext.BaseDirectory}/{filename}";
                            if (!File.Exists(filename))
                            {
                                Console.WriteLine($"Local Mapping File for Script '{job.Script}' Dost Not Exist in File System. Job is about to Fail.");
                            }
                        }
                        else
                        {
                            Console.WriteLine($"No Local Mapping Found for Script '{job.Script}'. Job is about to Fail.");
                            Debugger.Break();
                            await scheduler.CompleteScriptJob(message, false);
                            continue;
                        }
                    }
                    else
                    {
                        scriptFileObj = job.Script.ParseS3URI();
                        if (!await s3.FileExists(scriptFileObj.Key, scriptFileObj.BucketName))
                        {
                            await scheduler.CompleteScriptJob(message, false);
                            continue;
                        }
                    }
                }
                catch(Exception ex)
                {
                    await scheduler.CompleteScriptJob(message, false);
                    continue;
                }
            }

            // read the script
            BrowserScripts browserScripts = null;
            if (options.TestMode)
            {
                var filename = options.TestScriptMapping[job.Script];
                if (filename.StartsWith(".")) filename = $"{AppContext.BaseDirectory}/{filename}";
                browserScripts = JsonConvert.DeserializeObject<BrowserScripts>(File.ReadAllText(filename));
            }
            else
            {
                browserScripts = await s3.ReadFromJson<BrowserScripts>(scriptFileObj.Key, scriptFileObj.BucketName);
            }
            

            // run each scripts

            Scripts = browserScripts;
            if(browserScripts.Data == null)
            {
                browserScripts.Data = new Dictionary<string, string>();
            }

            // load the JObject payload data from the messsage
            Jsons = new Dictionary<string, JObject>();
            if(job.Payload != null)
            {
                foreach(var property in job.Payload.Properties())
                {
                    if(property.HasValues)
                    {
                        // we will store JObject in Jsons and key-value pairs in Dictionary
                        switch (property.Value.Type)
                        {
                            case JTokenType.Object:
                                Jsons.Add(property.Name, property.Value<JObject>());
                                break;
                            case JTokenType.String:
                                browserScripts.Data.Add(property.Name, property.Value.Value<string>());
                                break;
                            case JTokenType.Integer:
                                browserScripts.Data.Add(property.Name, property.Value.Value<string>());
                                break;
                            case JTokenType.Float:
                                browserScripts.Data.Add(property.Name, property.Value.Value<string>());
                                break;
                            case JTokenType.Boolean:
                                browserScripts.Data.Add(property.Name, property.Value.Value<string>());
                                break;
                            case JTokenType.Date:
                                browserScripts.Data.Add(property.Name, property.Value.Value<string>());
                                break;
                            default:
                                Console.Write($"Payload Property {property.Name} Ignored. Property type {property.Value.Type} is not supported!");
                                break;
                        }
                    }
                }
            }

            foreach (var key in Scripts.Run)
            {
                RunScript(key, browserScripts.Data);
                switch (browserScripts.Data["success"])
                {
                    case "true":
                        scheduler.CompleteScriptJob(message, true).Wait();
                        break;
                    case "false":
                        scheduler.CompleteScriptJob(message, false).Wait();
                        break;
                }
            }

            return true;
        }
    }
}
