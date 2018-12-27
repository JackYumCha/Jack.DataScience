using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Http.Jwt
{
    public static class JwtEncoderExtensions
    {
        public static Dictionary<string, string> ToDictionary<TRole>(this JwtTokenBase<TRole> token) where TRole: struct
             => new Dictionary<string, string>()
             {
                         {nameof(JwtTokenBase<TRole>.Id), token.Id},
                         {nameof(JwtTokenBase<TRole>.Name), token.Name },
                         {nameof(JwtTokenBase<TRole>.Role), token.Role.ToString() },
                         {nameof(JwtTokenBase<TRole>.ExpiringDate), token.ExpiringDate.ToString("yyyyMMddHHmmss")}
             };
    }
}
