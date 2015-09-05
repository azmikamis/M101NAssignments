using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Assignments
{
    public class Week53Assignment : IAssignment
    {
        public void doAssignment()
        {
            MainAsync().GetAwaiter().GetResult();
        }

        private async Task MainAsync()
        {
            var connStr = "mongodb://localhost:27017";
            var client = new MongoClient(connStr);
            var db = client.GetDatabase("test");
            var col = db.GetCollection<Student>("grades");
            await db.DropCollectionAsync("grades");

            string assignmentDir = @"handouts\homework_5_3";
            string inputFileName = "grades.json";
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

            var agg = col.Aggregate()
                .Unwind(x => x.Scores)
                .Match("{ 'scores.type' : { $in: ['exam','homework'] } }")
                .Group("{ _id : { class : '$class_id', student : '$student_id' }, studentavg : { $avg: '$scores.score' } }")
                .Group("{ _id : '$_id.class' , classavg: { $avg: '$studentavg' } }")
                .Sort("{ classavg : -1 }")
                .Limit(1)
                ;

            var cmd = String.Format("db.grades.{0}", agg.ToString());
            var results = await agg.ToListAsync();
        }
    }

    public class Student
    {
        [BsonId]
        public ObjectId Id { get; set; }
        [BsonElement("student_id")]
        public int Student_Id { get; set; }
        [BsonElement("class_id")]
        public int Class_Id { get; set; }
        [BsonElement("scores")]
        public List<Score> Scores { get; set; }
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
