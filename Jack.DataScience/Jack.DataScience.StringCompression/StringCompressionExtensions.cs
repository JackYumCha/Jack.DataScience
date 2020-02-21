using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace Jack.DataScience.StringCompression
{
    public static class StringCompressionExtensions
    {
        private static char[] AvailableChars = new char[] {
           ' ','!','#','$','%','&','\'','(',')','*','+',',','-','.','/','0','1','2','3','4','5','6','7','8','9',':',';','<','=','>','?','@','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z','[',']','^','_','`','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','{','|','}','~'
        };
        private static int[] MappingArray = new int[]
        {
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,0,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92
        };
        private static int AvailableLength = AvailableChars.Length;
        private static int AvailableSquare = AvailableLength * AvailableLength;
        private static double Log256 = Math.Log(256d);
        private static double LogAvailableLength = Math.Log(AvailableLength);


        public static string CompressBase64(this string value)
        {
            if (value == null) return null;
            if (value.Length == 0) return "";
            return Convert.ToBase64String(value.CompressBytes());
        }

        public static string DecompressBase64(this string value)
        {
            if (value == null) return null;
            if (value.Length == 0) return "";
            return Convert.FromBase64String(value).DecompressBytes();
        }

        private static byte[] CompressBytes(this string value)
        {
            using (MemoryStream memStream = new MemoryStream())
            {
                using (GZipStream zipStream = new GZipStream(memStream, CompressionMode.Compress))
                {
                    using(StreamWriter streamWriter = new StreamWriter(zipStream, Encoding.UTF8))
                    {
                        streamWriter.Write(value);
                        streamWriter.Flush();
                        return memStream.ToArray();
                    }
                }
            }
        }

        private static string DecompressBytes(this byte[] bytes)
        {
            using (MemoryStream memStream = new MemoryStream(bytes))
            {
                using (GZipStream zipStream = new GZipStream(memStream, CompressionMode.Decompress))
                {
                    using (StreamReader streamReader = new StreamReader(zipStream, Encoding.UTF8))
                    {
                        return streamReader.ReadToEnd();
                    }
                }
            }
        }

        public static string EncodeAsJsonValue(this string value)
        {
            if (value == null) return null;
            if (value.Length == 0) return "";
            byte[] bytes = Encoding.UTF8.GetBytes(value);
            int outputLength = (int)Math.Ceiling(bytes.Length * Log256 / LogAvailableLength);

            // abcdef....
            // a * 256 / 94 -> x1
            // ((a * 256 % 94) * 256 + b) / 94 -> x2
            // ((a * 256 % 94) * 256 + b) % 94 -> x3
            // a * 256 mode 94
            char[] chars = new char[outputLength];
            int sum = 0, max = 1, charIndex = 0;
            foreach (byte b in bytes)
            {
                sum = sum * 256 + b;
                max = max * 256 + 255;
                while(max >= AvailableLength)
                {
                    int sumbefore = sum;
                    if(max >= AvailableSquare)
                    {
                        chars[charIndex] = AvailableChars[sum / AvailableSquare];
                        max = max % AvailableSquare;
                        sum = sum % AvailableSquare;
                        charIndex++;
                    }
                    chars[charIndex] = AvailableChars[sum / AvailableLength];
                    max = max % AvailableLength;
                    sum = sum % AvailableLength;
                    charIndex++;
                }
            }
            if(max > 0)
            {
                chars[charIndex] = AvailableChars[sum / AvailableLength];
                charIndex++;
            }
            return new string(chars);
        }

        public static string DecodeFromJsonValue(this string value)
        {
            if (value == null) return null;
            if (value.Length == 0) return "";
            int length = value.Length, bytesLength = (int)Math.Ceiling(length * LogAvailableLength / Log256);
            int sum = 0, max = 1, byteIndex = 0;
            byte[] bytes = new byte[bytesLength];
            for(int i = length - 1; i >= 0; i--)
            {
                sum = sum * AvailableLength + MappingArray[value[i]];
                max *= AvailableLength;
                while(max >= 256)
                {
                    max = max / 256;
                    bytes[byteIndex] = (byte)(sum % 256);
                    sum = sum / 256;
                    byteIndex++;
                }
            }
            if(max > 0)
            {
                bytes[byteIndex] = (byte)(sum % 256);
            }
            return Encoding.UTF8.GetString(bytes);
        }

    }
}
