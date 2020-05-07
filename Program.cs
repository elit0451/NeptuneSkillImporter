using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using CsvHelper;
using NeptuneSkillImporter.Database;
using NeptuneSkillImporter.Helpers;
using NeptuneSkillImporter.Models;

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
            const string endpoint = "localhost";
            const int port = 8182;

            try
            {
                var skills = new List<Skill>();

                // This uses the default Neptune and Gremlin port, 8182
                var gremlinConnector = new GremlinConnector(endpoint, port);

                var graph = gremlinConnector.GetGraph();

                var gremlinDB = new GremlinDB(graph);

                // Drop entire DB
                gremlinDB.Drop();

                // get job posts
                var jobPosts = JobPostRepo.GetJobPosts();

                // load csv data for skills
                skills = LoadDataToMemory();
                // skills into DB
                gremlinDB.InsertNodes(skills);

                // edges into DB
                IJobPostProcessor jobPostProcessor = new JobPostProcessor();
                var jobPostsSkills = jobPostProcessor.ProcessJobPosts(skills, jobPosts);
                gremlinDB.InsertEdges(jobPostsSkills);

                // get related skills
                const string skillNameForSearch = "c#";
                const int limit = 10;
                var relatedSkills = gremlinDB.GetRelatedSkills(skillNameForSearch, limit);

                Console.WriteLine($"Top {limit} skills related to {skillNameForSearch}:\n");
                foreach (var skill in relatedSkills)
                    Console.WriteLine($"Name: {skill.Name}, Category: {skill.Category}, Weight: {skill.Weight}");

                Console.WriteLine("\n\nTotal number of skills: {0}", gremlinDB.CountNodes());

                Console.WriteLine("Finished");
            }
            catch (Exception e)
            {
                Console.WriteLine("{0}", e);
            }
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
    }
}
