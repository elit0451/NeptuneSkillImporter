using System.Collections.Generic;
using System.Text.RegularExpressions;
using NeptuneSkillImporter.Models;

namespace NeptuneSkillImporter.Helpers
{
    public class JobPostProcessor : IJobPostProcessor
    {
        public IEnumerable<IEnumerable<Skill>> ProcessJobPosts(IEnumerable<Skill> skills, IEnumerable<JobPost> jobPosts)
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
    }
}