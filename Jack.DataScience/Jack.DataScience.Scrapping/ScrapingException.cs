using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Scrapping
{
    public class ScrapingException: Exception
    {
        public ScrapingException(string message) : base(message) { }
    }
}
