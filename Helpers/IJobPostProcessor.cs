using System.Collections.Generic;
using NeptuneSkillImporter.Models;

namespace NeptuneSkillImporter.Helpers
{
    public interface IJobPostProcessor
    {
         IEnumerable<IEnumerable<Skill>> ProcessJobPosts(IEnumerable<Skill> skills, IEnumerable<JobPost> jobPosts);
    }
}