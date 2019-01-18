using System;
using System.Text;
using System.Security.Cryptography;

namespace Jack.DataScience.Http.Password
{
    public static class MD5Extensions
    {
        private static readonly MD5 md5 = MD5.Create();
        private static readonly Encoding encoding = Encoding.UTF8;
        public static string ToMD5Hash(this string value)
        {
            return BitConverter.ToString(md5.ComputeHash(encoding.GetBytes(value))).Replace("-", "").ToLower();
        }
    }
}
