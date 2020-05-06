using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using CsvHelper;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process.Traversal;
using Newtonsoft.Json;
using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;

namespace NeptuneSkillImporter
{
    public class Program
    {
        public static void Main()
        {
            new Program().Run();
        }

        public void Run()
        {
            try
            {
                const string endpoint = "localhost";
                var skills = new List<Skill>();

                // This uses the default Neptune and Gremlin port, 8182
                var gremlinServer = new GremlinServer(endpoint, 8182);
                var gremlinClient = new GremlinClient(gremlinServer);

                var graph = Traversal().WithRemote(new DriverRemoteConnection(gremlinClient));

                // Drop entire DB
                graph.V().Drop().Iterate();

                // get job posts
                var jobPosts = JobPostRepo.GetJobPosts();

                // load csv data for skills
                skills = LoadDataToMemory();
                // skills into DB
                InsertDataInDB(skills, graph);

                // edges into DB
                var jobPostsSkills = ProcessJobPosts(skills, jobPosts);
                InsertEdgesInDB(jobPostsSkills, graph);

                // get related skills
                const string skillNameForSearch = "c#";
                const int limit = 10;
                var relatedSkills = GetRelatedSkills(skillNameForSearch, limit, graph);

                Console.WriteLine($"Top {limit} skills related to {skillNameForSearch}:\n");
                foreach(var skill in relatedSkills)
                    Console.WriteLine($"Name: {skill.Name}, Category: {skill.Category}, Weight: {skill.Weight}");

                RunQueryAsync(gremlinClient).Wait();

                Console.WriteLine("Finished");
            }
            catch (Exception e)
            {
                Console.WriteLine("{0}", e);
            }
        }

        private async Task RunQueryAsync(GremlinClient gremlinClient)
        {
            var count = await gremlinClient.SubmitWithSingleResultAsync<long>("g.V().count().next()");

            Console.WriteLine("\n\nTotal number of skills: {0}", count);
        }

        public List<Skill> LoadDataToMemory()
        {
            //TODO: add the file to S3 bucket and get it URI
            const string filePath = "./data/skills-dataset.csv";
            List<Skill> records = null;

            using (StreamReader sr = new StreamReader(filePath, Encoding.UTF8))
            {
                using (var csv = new CsvReader(sr, CultureInfo.InvariantCulture))
                {
                    csv.Configuration.BadDataFound = null;
                    csv.Configuration.HasHeaderRecord = true;

                    records = csv.GetRecords<Skill>().ToList();
                }
            }

            return records;
        }

        public void InsertDataInDB(IEnumerable<Skill> skills, GraphTraversalSource graph)
        {
            foreach (var skill in skills)
            {
                graph.AddV("skill").Property("name", skill.Name).Property("category", skill.Category).Next();
            }
        }

        public List<IEnumerable<Skill>> ProcessJobPosts(IEnumerable<Skill> skills, List<JobPost> jobPosts)
        {
            var processedSkills = new List<IEnumerable<Skill>>();

            foreach (var jobPost in jobPosts)
            {
                var foundSkills = new List<Skill>();

                if (jobPost.Keywords.Count != 0)
                {
                    foreach (var keyword in jobPost.Keywords)
                    {
                        foundSkills.Add(new Skill()
                        {
                            Name = keyword,
                            Weight = 10
                        });
                    }
                }
                else
                {
                    foreach (var skill in skills)
                    {
                        var skillName = Regex.Escape(skill.Name);
                        var pattern = $"[^A-Za-z]({skillName})[^A-Za-z]";
                        Regex r = new Regex(pattern, RegexOptions.Multiline);

                        if (r.IsMatch(jobPost.Header) || r.IsMatch(jobPost.Body))
                        {
                            skill.Weight = 1;
                            foundSkills.Add(skill);
                        }
                    }
                }

                processedSkills.Add(foundSkills);
            }

            return processedSkills;
        }

        public void InsertEdgesInDB(IEnumerable<IEnumerable<Skill>> jobPostsSkills, GraphTraversalSource graph)
        {
            foreach (var jobPostSkills in jobPostsSkills)
            {
                Skill[] skills = jobPostSkills.ToArray();

                for (int i = 0; i < skills.Length - 1; i++)
                {
                    for (int j = i + 1; j < skills.Length; j++)
                    {
                        // find vertices with the same skill name from the graph
                        var v1 = graph.V().HasLabel("skill").Has("name", skills[i].Name).Next();
                        var v2 = graph.V().HasLabel("skill").Has("name", skills[j].Name).Next();

                        // insert biderectional edges between 2 skills
                        // set initial edge weight count to 0
                        graph.V(v2).As("v2").V(v1).As("v1").Not(__.Both("weight").Where(P.Eq("v2")))
                            .AddE("weight").Property("count", 0).From("v2").To("v1").OutV()
                            .AddE("weight").Property("count", 0).From("v1").To("v2").Iterate();

                        // increase edge weight count when 2 skills are found in the same job post
                        graph.V(v1).BothE().Where(__.BothV().HasId(v2.Id)).Property("count", __.Union<int>(__.Values<int>("count"), __.Constant(skills[i].Weight)).Sum<int>()).Next();
                    }
                }
            }
        }

        public List<Skill> GetRelatedSkills(string skillName, int limit, GraphTraversalSource graph)
        {
            // find vertices with the same skill name from the graph
            var v1 = graph.V().HasLabel("skill").Has("name", skillName).Next();

            // find top {limit} related skills 
            var relatedSkills = graph.V(v1).OutE().As("e").Order().By("count", Order.Decr).InV().Limit<int>(limit).Project<object>("name", "category", "weight").By("name").By("category").By(__.Select<object>("e").Values<object>("count")).ToList();
            var jsonRelatedSkills = JsonConvert.DeserializeObject<List<Skill>>(JsonConvert.SerializeObject(relatedSkills));

            return jsonRelatedSkills;
        }
    }
}
