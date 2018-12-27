using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Data;
using System.Globalization;

namespace Jack.DataScience.WPF.Controls
{
    public class StringListConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            var list = value as List<string>;
            if (list is List<string>)
            {
                return string.Join(" ", list);
            }
            return "";
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            var stringValue = value as string;
            if (stringValue is string)
            {
                return stringValue.Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries).ToList();
            }
            return new List<string>();
        }
    }
}
