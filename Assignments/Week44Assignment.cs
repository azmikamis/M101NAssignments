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
    public class Week44Assignment : IAssignment
    {
        public void doAssignment()
        {
            MainAsync().GetAwaiter().GetResult();
        }

        private async Task MainAsync()
        {
            var connStr = "mongodb://localhost:27017";
            var client = new MongoClient(connStr);
            var db = client.GetDatabase("m101");
            var col = db.GetCollection<BsonDocument>("profile");
            //await db.DropCollectionAsync("profile");

            var list = await col
                .Find("{op:'query',ns:'school2.students'},{millis:1,_id:0}")
                .Sort("{millis:-1}")
                .Limit(1)
                .ToListAsync();
        }
    }
}
