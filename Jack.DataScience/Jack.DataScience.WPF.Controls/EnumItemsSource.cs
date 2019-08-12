using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jack.DataScience.WPF.Controls
{
    public class EnumItemsSource: List<object>
    {
        private Type type;
        public Type Type
        {
            get => type;
            set
            {
                type = value;
                Clear();
                if(type != null && type.IsEnum)
                {
                    foreach (var item in Enum.GetValues(type)) Add(item);
                }
            }
        }
    }
}
