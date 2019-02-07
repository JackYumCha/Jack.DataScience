using System.Collections.Generic;
using JWT;
using JWT.Algorithms;
using JWT.Serializers;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;

namespace Jack.DataScience.Http.Jwt
{
    /// <summary>
    /// this encoder implements the JWT encoding. it can encode a dictionary into JWT and decode it
    /// </summary>
    public class JwtObjectEncoder
    {
        private readonly string secret;
        private readonly IJwtAlgorithm algorithm;
        private readonly IJsonSerializer serializer;
        private readonly IBase64UrlEncoder urlEncoder;
        private readonly IDateTimeProvider provider;
        private readonly IJwtValidator validator;
        private readonly IJwtEncoder encoder;
        private readonly IJwtDecoder decoder;
        private const string payloadKey = "payload";
        private static readonly JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings()
        {
            DateFormatString = "yyyy-MM-ddTHH:mm:ss.FFFFFFFK",
            Converters = { new StringEnumConverter() }
        };
        public JwtObjectEncoder(JwtSecretOptions jwtSecretOptions)
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

        public string Encode<T>(T payload)
        {
            Dictionary<string, string> jwt = new Dictionary<string, string>();
            jwt.Add(payloadKey, JsonConvert.SerializeObject(payload, jsonSerializerSettings));
            return encoder.Encode(jwt, secret);
        }

        public T Decode<T>(string jwt)
        {
            var jDict = JsonConvert.DeserializeObject<JObject>(decoder.Decode(jwt));
            return jDict.GetValue(payloadKey).ToObject<T>();
        }
    }
}
