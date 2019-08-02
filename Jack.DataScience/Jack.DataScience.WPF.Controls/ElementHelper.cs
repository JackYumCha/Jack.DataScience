using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jack.DataScience.WPF.Controls
{
    public static class ElementHelper
    {
        public static T As<T>(this object value) where T: class => value as T;
    }
}
