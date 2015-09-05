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
    public class Week51Assignment : IAssignment
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
            var col = db.GetCollection<Post>("posts");

            var agg = col.Aggregate()
                .Unwind(x => x.Comments)
                .Group("{ _id : '$comments.author', count : { $sum : 1 } }")
                .Sort("{ count : -1 }")
                .Limit(1)
                ;

            var cmd = String.Format("db.posts.{0}", agg.ToString());
            var results = await agg.ToListAsync();
        }
    }

    public class Post
    {
        [BsonId]
        public ObjectId Id { get; set; }
        [BsonElement("body")]
        public string Body { get; set; }
        [BsonElement("permalink")]
        public string Permalink { get; set; }
        [BsonElement("author")]
        public string Author { get; set; }
        [BsonElement("title")]
        public string Title { get; set; }
        [BsonElement("tags")]
        public List<string> Tags { get; set; }
        [BsonElement("comments")]
        public List<Comment> Comments { get; set; }
    }

    public class Comment
    {
        [BsonElement("body")]
        public string Body { get; set; }
        [BsonElement("email")]
        public string Email { get; set; }
        [BsonElement("author")]
        public string Author { get; set; }
    }
}
