using System.Collections.Generic;
using Newtonsoft.Json;

namespace NeptuneSkillImporter.Models
{
    public class JobPost
    {
        [JsonProperty("Title")]
        public string Header { get; set; }
        public List<string> Keywords { get; set; }

        [JsonProperty("FullJobPost")]
        public string Body { get; set; }

        public JobPost(string header, string body)
        {
            Header = header.ToLower();
            Body = body.ToLower();
            Keywords = new List<string>();
        }
    }
}