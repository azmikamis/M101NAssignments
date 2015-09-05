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
    public class Week54Assignment : IAssignment
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
            var col = db.GetCollection<ZipEntry1>("zips");
            //await db.DropCollectionAsync("zips");

            //string assignmentDir = @"handouts\homework_5_4";
            //string inputFileName = "zips.json";
            //string inputFilePath = Path.Combine(assignmentDir, inputFileName);

            //using (var streamReader = new StreamReader(inputFilePath))
            //{
            //    string line;
            //    while ((line = await streamReader.ReadLineAsync()) != null)
            //    {
            //        var document = BsonSerializer.Deserialize<ZipEntry1>(line);
            //        await col.InsertOneAsync(document);
            //    }
            //}

            var agg = col.Aggregate()
                .Match(@"{city: /^\d.*$/}")
                .Group(x => true, g => new { TotPop = g.Sum(x => x.Population) })
                ;

            var cmd = String.Format("db.zips.{0}", agg.ToString());
            var results = await agg.ToListAsync();
        }
    }

    public class ZipEntry1
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
