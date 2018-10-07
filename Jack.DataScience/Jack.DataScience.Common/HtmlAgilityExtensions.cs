using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using HtmlAgilityPack;
using System.Net.Http;

namespace Jack.DataScience.Http
{
    public static class HtmlAgilityExtensions
    {
        public static HtmlDocument GetHtmlDocument(this HttpClient httpClient, string url)
        {
            var response = httpClient.GetAsync(url).GetAwaiter().GetResult();
            var html = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
            HtmlDocument htmlDocument = new HtmlDocument();
            htmlDocument.LoadHtml(html);
            return htmlDocument;
        }

        public static HtmlDocument PostForHtmlDocument(this HttpClient httpClient, string url, 
            FormUrlEncodedContent formUrlEncodedContent)
        {
            var response = httpClient.PostAsync(url, formUrlEncodedContent).GetAwaiter().GetResult();
            var html = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
            HtmlDocument htmlDocument = new HtmlDocument();
            htmlDocument.LoadHtml(html);
            return htmlDocument;
        }

        public static HtmlDocument PostForHtmlDocument(this HttpClient httpClient, string url,
            IDictionary<string, string> formData)
        {
            FormUrlEncodedContent formUrlEncodedContent = new FormUrlEncodedContent(formData);
            return httpClient.PostForHtmlDocument(url, formUrlEncodedContent);
        }

        public static HttpResponseMessage PostDictionary(this HttpClient httpClient, string url,
    IDictionary<string, string> formData)
        {
            FormUrlEncodedContent formUrlEncodedContent = new FormUrlEncodedContent(formData);
            return httpClient.PostAsync(url, formUrlEncodedContent).GetAwaiter().GetResult();
        }

        public static Dictionary<string, string> BuildForm(this HtmlNode htmlNode)
        {
            // find all the input and text area element children for this node
            var inputs = htmlNode
                .DescendantsAndSelf()
                .Where(n => n.Name.ToLower() == "input"
                    && n.GetAttributeValue("name", "***---***") != "***---***"
                    && n.GetAttributeValue("value", "***---***") != "***---***")
                .Select(n => 
                    new KeyValuePair<string, string>(
                        n.GetAttributeValue("name", "***---***"), n.GetAttributeValue("value", "***---***")))
                .ToList();

            var selects = htmlNode
                .DescendantsAndSelf()
                .Where(n => n.Name.ToLower() == "select" && n.GetAttributeValue("name", "***---***") != "***---***")
                .Select(n => new KeyValuePair<string, string>(
                    n.GetAttributeValue("name", "***---***"),
                    n.DescendantsAndSelf().Any(c => c.Name.ToLower() == "option" && c.GetAttributeValue("selected", "***---***") != "***---***") ?
                    n.DescendantsAndSelf().First(c => c.Name.ToLower() == "option" && c.GetAttributeValue("selected", "***---***") != "***---***").GetAttributeValue("value", "") : ""
                    )).ToList();

            Dictionary<string, string> formData = new Dictionary<string, string>();

            foreach(var kvp in inputs)
            {
                if (formData.ContainsKey(kvp.Key))
                {
                    formData[kvp.Key] = kvp.Value;
                }
                else
                {
                    formData.Add(kvp.Key, kvp.Value);
                }
            }

            foreach (var kvp in selects)
            {
                if (formData.ContainsKey(kvp.Key))
                {
                    formData[kvp.Key] = kvp.Value;
                }
                else
                {
                    formData.Add(kvp.Key, kvp.Value);
                }
            }

            return formData;
        }

        public static string ViewDictionary(this Dictionary<string, string> dictionary)
        {
            StringBuilder stringBuilder = new StringBuilder();

            foreach(var kvp in dictionary)
            {
                stringBuilder.AppendLine($"{kvp.Key}: {kvp.Value}");
            }
            return stringBuilder.ToString();
        }

        public static string CompareDictionary(this Dictionary<string, string> dictionary, string dictionaryFormData, string name = "dictDefault")
        {
            var lines = dictionaryFormData.Split(new char[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);
            Dictionary<string, string> dict = new Dictionary<string, string>();
            foreach(var line in lines)
            {
                var fields = line.Split(new string[] { ": " }, StringSplitOptions.RemoveEmptyEntries);
                if (dict.ContainsKey(fields[0]))
                {
                    dict[fields[0]] += " --Duplicated-- " + fields[1];
                }
                else
                {
                    dict.Add(fields[0], fields[1]);
                }
                
            }

            StringBuilder stb = new StringBuilder();

            stb.AppendLine("*** Same ***");
            foreach(var k in dictionary.Keys.Where(key => dict.ContainsKey(key) && dictionary[key] == dict[key]).ToList())
            {
                stb.AppendLine($"Key: {k}, Value: {dictionary[k]}, Ref: {dict[k]}");
            }

            stb.AppendLine();
            stb.AppendLine("*** Different ***");
            foreach (var k in dictionary.Keys.Where(key => dict.ContainsKey(key) && dictionary[key] != dict[key]).ToList())
            {
                stb.AppendLine($"Key: {k}, Value: {dictionary[k]}, Ref: {dict[k]}");
            }

            stb.AppendLine();
            stb.AppendLine("*** Missing ***");
            foreach (var k in dict.Keys.Where(key => !dictionary.ContainsKey(key)).ToList())
            {
                stb.AppendLine($"Key: {k}, Value: N/A, Ref: {dict[k]}");
            }

            stb.AppendLine();
            stb.AppendLine("*** Extra ***");
            foreach (var k in dictionary.Keys.Where(key => !dict.ContainsKey(key)).ToList())
            {
                stb.AppendLine($"Key: {k}, Value: {dictionary[k]}, Ref: N/A");
            }

            stb.AppendLine();
            stb.AppendLine("*** Fix ***");
            foreach (var k in dict.Keys.Where(key => !dictionary.ContainsKey(key)).ToList())
            {
                stb.AppendLine($"{name}.Add(\"{k}\",\"{dict[k]}\");");
            }
            foreach (var k in dictionary.Keys.Where(key => !dict.ContainsKey(key)).ToList())
            {
                stb.AppendLine($"{name}.Remove(\"{k}\");");
            }

            return stb.ToString();
        }
    }
}
