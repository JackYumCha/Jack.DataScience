using System.Collections.Generic;
using JWT;
using JWT.Algorithms;
using JWT.Serializers;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Jack.DataScience.Http.Jwt
{
    /// <summary>
    /// this encoder implements the JWT encoding. it can encode a dictionary into JWT and decode it
    /// </summary>
    public class RoleJwtEncoder
    {
        private readonly string secret;
        private readonly IJwtAlgorithm algorithm;
        private readonly IJsonSerializer serializer;
        private readonly IBase64UrlEncoder urlEncoder;
        private readonly IDateTimeProvider provider;
        private readonly IJwtValidator validator;
        private readonly IJwtEncoder encoder;
        private readonly IJwtDecoder decoder;
        private static readonly JsonSerializer jsonSerializer = new JsonSerializer()
        {
            DateFormatString = "yyyy-MM-ddTHH:mm:ss.FFFFFFFK",
        };
        public RoleJwtEncoder(JwtSecretOptions jwtSecretOptions)
        {
            secret = jwtSecretOptions.Secret;
            algorithm = new HMACSHA256Algorithm();
            serializer = new JsonNetSerializer();
            urlEncoder = new JwtBase64UrlEncoder();
            provider = new UtcDateTimeProvider();
            validator = new JwtValidator(serializer, provider);
            encoder = new JwtEncoder(algorithm, serializer, urlEncoder);
            decoder = new JwtDecoder(serializer, validator, urlEncoder);
        }

        public string Encode(IDictionary<string, string> payload)
        {
            return encoder.Encode(payload, secret);
        }

        public JObject Decode(string jwt)
        {
            return JsonConvert.DeserializeObject<JObject>(decoder.Decode(jwt));
        }

        public string Encode<TRole>(JwtTokenBase<TRole> token) where TRole: struct
        {
            var payload = token.ToDictionary();
            return Encode(payload);
        }

        public JwtTokenBase<TRole> Decode<TRole>(string jwt) where TRole : struct
        {
            var token = Decode(jwt);
            return token.ToObject<JwtTokenBase<TRole>>();
        }
    }
}
