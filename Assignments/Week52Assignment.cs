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
    public class Week52Assignment : IAssignment
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
            var col = db.GetCollection<ZipEntry>("zips");
            await db.DropCollectionAsync("zips");

            string assignmentDir = @"handouts\homework_5_2";
            string inputFileName = "small_zips.json";
            string inputFilePath = Path.Combine(assignmentDir, inputFileName);

            using (var streamReader = new StreamReader(inputFilePath))
            {
                string line;
                while ((line = await streamReader.ReadLineAsync()) != null)
                {
                    var document = BsonSerializer.Deserialize<ZipEntry>(line);
                    await col.InsertOneAsync(document);
                }
            }

            var agg = col.Aggregate()
                .Match(x => new[] { "CA", "NY" }.Contains(x.State))
                .Group(x => x.City, g => new { Count = g.Count(), TotalPop = g.Sum(x => x.Population) })
                .Match(x => x.TotalPop > 25 * 1000)
                .Group(x => true, g => new { AvgPop = g.Average(x => x.TotalPop) })
                ;

            var cmd = String.Format("db.zips.{0}", agg.ToString());
            var results = await agg.ToListAsync();
        }
    }

    public class ZipEntry
    {
        [BsonId]
        public string Id { get; set; }
        [BsonElement("city")]
        public string City { get; set; }
        [BsonElement("loc")]
        public double[] Location { get; set; }
        [BsonElement("pop")]
        public int Population { get; set; }
        [BsonElement("state")]
        public string State { get; set; }
    }
}
