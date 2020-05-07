using System.Collections.Generic;
using System.Linq;
using Gremlin.Net.Process.Traversal;
using NeptuneSkillImporter.Models;
using Newtonsoft.Json;

namespace NeptuneSkillImporter.Database
{
    public class GremlinDB
    {
        private readonly GraphTraversalSource _graph;

        public GremlinDB(GraphTraversalSource graph)
        {
            _graph = graph;
        }

        public void Drop()
        {
            _graph.V().Drop().Iterate();
        }

        public void InsertNodes(List<Skill> skills)
        {
            foreach (var skill in skills)
            {
                _graph.AddV("skill").Property("name", skill.Name).Property("category", skill.Category).Next();
            }
        }

        public void InsertEdges(IEnumerable<IEnumerable<Skill>> jobPostsSkills)
        {
            foreach (var jobPostSkills in jobPostsSkills)
            {
                Skill[] skills = jobPostSkills.ToArray();

                for (int i = 0; i < skills.Length - 1; i++)
                {
                    for (int j = i + 1; j < skills.Length; j++)
                    {
                        // find vertices with the same skill name from the graph
                        var v1 = _graph.V().HasLabel("skill").Has("name", skills[i].Name).Next();
                        var v2 = _graph.V().HasLabel("skill").Has("name", skills[j].Name).Next();

                        // insert biderectional edges between 2 skills
                        // set initial edge weight count to 0
                        _graph.V(v2).As("v2").V(v1).As("v1").Not(__.Both("weight").Where(P.Eq("v2")))
                            .AddE("weight").Property("count", 0).From("v2").To("v1").OutV()
                            .AddE("weight").Property("count", 0).From("v1").To("v2").Iterate();

                        // increase edge weight count when 2 skills are found in the same job post
                        _graph.V(v1).BothE().Where(__.BothV().HasId(v2.Id))
                            .Property("count", __.Union<int>(__.Values<int>("count"), __.Constant(skills[i].Weight)).Sum<int>()).Next();
                    }
                }
            }
        }

        public int CountNodes()
        {
            var count = _graph.V().Count().Next();

            return (int) count;
        }

        public List<Skill> GetRelatedSkills(string skillName, int limit)
        {
            // find vertices with the same skill name from the graph
            var v1 = _graph.V().HasLabel("skill").Has("name", skillName).Next();

            // find top {limit} related skills 
            var relatedSkills = _graph.V(v1).OutE().As("e").Order().By("count", Order.Decr).InV().Limit<int>(limit)
                .Project<object>("name", "category", "weight").By("name").By("category").By(__.Select<object>("e")
                .Values<object>("count")).ToList();

            var jsonRelatedSkills = JsonConvert.DeserializeObject<List<Skill>>(JsonConvert.SerializeObject(relatedSkills));

            return jsonRelatedSkills;
        }
    }
}