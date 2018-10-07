using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MediGraph.API.AutoFac
{
    public class AuthOptions
    {
        public int TokenExpiringDays { get; set; }
        public string JWTCookieKey { get; set; }
    }
}
