using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Assignments
{
    public class Week3Assignment : IAssignment
    {
        public void doAssignment()
        {
            MainAsync().GetAwaiter().GetResult();
        }

        private async Task MainAsync()
        {
            var connStr = "mongodb://localhost:27017";
            var client = new MongoClient(connStr);
            var db = client.GetDatabase("school");
            var col = db.GetCollection<Student>("students");
            await db.DropCollectionAsync("students");

            string assignmentDir = @"handouts\homework_3_1";
            string inputFileName = "students.json";
            string inputFilePath = Path.Combine(assignmentDir, inputFileName);

            using (var streamReader = new StreamReader(inputFilePath))
            {
                string line;
                while ((line = await streamReader.ReadLineAsync()) != null)
                {
                    var document = BsonSerializer.Deserialize<Student>(line);
                    await col.InsertOneAsync(document);
                }
            }

            var list = await col
                .Find(x => true)
                .ToListAsync();

            foreach (var doc in list)
            {
                double lowestHomeworkScore = doc.Scores.FindAll(x => x.Type == "homework").Min(x => x.ScoreValue);
                await col
                    .UpdateOneAsync(x => x.Id == doc.Id,
                        Builders<Student>.Update.PullFilter(p => p.Scores,
                            f => f.Type == "homework" && f.ScoreValue == lowestHomeworkScore));
            }

            var agg = col.Aggregate()
                .Unwind(x => x.Scores)
                .Group("{ _id : '$_id', average : { $avg: '$scores.score' } }")
                .Sort("{ average : -1 }")
                .Limit(1)
                ;

            var results = await agg.ToListAsync();

            foreach (var doc in results)
            {
                Console.WriteLine("Answer:{0}", doc[0]);
                break;
            }
        }

        public class Student
        {
            [BsonId]
            public int Id { get; set; }
            [BsonElement("name")]
            public string Name { get; set; }
            [BsonElement("scores")]
            public List<Score> Scores { get; set; }
            public override string ToString()
            {
                return string.Format("Name:{0}\n\tScores:{1}", Name, String.Join(", ", Scores));
            }
        }

        public class Score
        {
            [BsonElement("score")]
            public double ScoreValue { get; set; }
            [BsonElement("type")]
            public string Type { get; set; }
            public override string ToString()
            {
                return string.Format("Score:{0}, Type:{1}", ScoreValue, Type);
            }
        }
    }
}
