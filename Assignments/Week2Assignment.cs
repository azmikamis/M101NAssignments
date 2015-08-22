using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Assignments
{
    public class Week2Assignment : IAssignment
    {
        public void doAssignment()
        {
            MainAsync().GetAwaiter().GetResult();
        }

        private async Task MainAsync()
        {
            var connStr = "mongodb://localhost:27017";
            var client = new MongoClient(connStr);
            var db = client.GetDatabase("students");
            var col = db.GetCollection<Student>("grades");
            await db.DropCollectionAsync("grades");

            string assignmentDir = @"handouts\homework_2_1";
            string inputFileName = "grades.json";
            string inputFilePath = Path.Combine(assignmentDir, inputFileName);

            using (var streamReader = new StreamReader(inputFilePath))
            {
                string line;
                while((line = await streamReader.ReadLineAsync()) != null)
                {
                    var doc = BsonSerializer.Deserialize<Student>(line);
                    await col.InsertOneAsync(doc);
                }
            }

            var agg1 = col.Aggregate()
                .Group(x => x.Student_Id, g => new { StudentId = g.Key, Average = g.Average(x => x.Score) })
                .SortByDescending(x => x.Average)
                .Limit(1)
                ;
            
            var results1 = await agg1.ToListAsync();

            foreach (var doc in results1)
            {
                Debug.Assert(doc.StudentId == 164);
                Debug.Assert(Math.Round(doc.Average, 1) == 89.3);
                break;
            }

            var agg2 = col.Aggregate()
                .Match(x => x.Score >= 65)
                .SortBy(x => x.Score)
                .Limit(1)
                ;

            var results2 = await agg2.ToListAsync();
            foreach (var doc in results2)
            {
                Debug.Assert(doc.Student_Id == 22);
                break;
            }

            var list = await col
                .Find(x => x.Type == "homework")
                .SortBy(x => x.Student_Id)
                .ThenBy(x => x.Score)
                .ToListAsync()
                ;

            int prev = -1;
            foreach (var doc in list)
            {
                if (doc.Student_Id != prev)
                {
                    prev = doc.Student_Id;
                    await col.DeleteOneAsync(x => x.Id == doc.Id);
                }
            }

            var results3 = await agg1.ToListAsync();
            foreach (var doc in results3)
            {
                Console.WriteLine("Answer:{0}", doc.StudentId);
                break;
            }
        }

        public class Student
        {
            [BsonId]
            public ObjectId Id { get; set; }
            [BsonElement("student_id")]
            public int Student_Id { get; set; }
            [BsonElement("type")]
            public string Type { get; set; }
            [BsonElement("score")]
            public double Score { get; set; }
            public override string ToString()
            {
                return string.Format("Student_Id:{0}, Type:{1}, Score:{2}", Student_Id, Type, Score);
            }
        }
    }
}
