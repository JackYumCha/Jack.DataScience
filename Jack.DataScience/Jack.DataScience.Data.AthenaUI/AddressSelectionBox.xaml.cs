using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;

namespace Jack.DataScience.Data.AthenaUI
{
    /// <summary>
    /// AddressSelectionBox.xaml 的交互逻辑
    /// </summary>
    public partial class AddressSelectionBox : Window
    {
        public AddressSelectionBox()
        {
            InitializeComponent();
        }

        public new string ShowDialog()
        {
            base.ShowDialog();
            var text = tbInput.Text;
            tbInput.Text = "";
            return DialogResult == true ? text : null;
        }

        public static string GetAddressInput(string value = "")
        {
            var box = new AddressSelectionBox();
            box.tbInput.Text = value;
            return box.ShowDialog();
        }

        private void DialogOK(object sender, RoutedEventArgs e)
        {
            DialogResult = true;
        }

        private void DialogCancel(object sender, RoutedEventArgs e)
        {
            DialogResult = false;
        }
    }
}
