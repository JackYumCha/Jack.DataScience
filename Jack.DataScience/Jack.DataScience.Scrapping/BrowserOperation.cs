using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Scrapping
{
    public class BrowserOperation
    {
        public ActionTypeEnum Action { get; set; }
        /// <summary>
        /// label for the action
        /// </summary>
        public string Label { get; set; }
        /// <summary>
        /// indicate if the "Then" operations should be run by all elements or per element. True for all
        /// </summary>
        public bool Batch { get; set; }
        /// <summary>
        /// run in driver mode
        /// </summary>
        public bool Driver { get; set; }
        /// <summary>
        /// indicate that this step should not be used
        /// </summary>
        public bool Disabled { get; set; }
        /// <summary>
        /// paramters
        /// GoTo -> Url: the url
        /// *Keys -> Keys: string value for send keys
        /// Get* -> Output Value key
        /// Ref: refer to the dynamic parameters,
        /// </summary>
        public List<string> Parameters { get; set; }
        public string Css { get; set; }
        /// <summary>
        /// following operations on the captured element (if captured)
        /// </summary>
        public List<BrowserOperation> Then { get; set; }
    }


}
