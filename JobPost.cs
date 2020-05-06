using System.Collections.Generic;

namespace NeptuneSkillImporter
{
    public class JobPost
    {
        public string S3Key { get; set; }
        public string Header { get; set; }
        public List<string> Keywords { get; set; }
        public string Body { get; set; }

        public JobPost(string header, string body)
        {
            Header = header.ToLower();
            Body = body.ToLower();
            Keywords = new List<string>();
        }
    }
}