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
    public class Week4Assignment : IAssignment
    {
        public void doAssignment()
        {
            MainAsync().GetAwaiter().GetResult();
        }

        private async Task MainAsync()
        {
            var connStr = "mongodb://localhost:27017";
            var client = new MongoClient(connStr);
            var db = client.GetDatabase("store");
            var col = db.GetCollection<Product>("products");
            await db.DropCollectionAsync("products");

            Random random = new Random();

            List<string> descriptions = new List<string>() { "aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii", "jjj" };
            List<string> categories = new List<string>() { "sports", "food", "clothing", "electronics" };
            List<string> brands = new List<string>() { "GE", "nike", "adidas", "toyota", "mitsubishi" };

            var docs = Enumerable.Range(0, 100)
                .Select(i => new Product
                {
                    Id = i,
                    SKU = String.Format("{0:D8}", i),
                    Price = random.NextDouble() * (100 - 1) + 1,
                    Description = descriptions[random.Next(descriptions.Count)],
                    Category = categories[random.Next(categories.Count)],
                    Brand = brands[random.Next(brands.Count)],
                    Reviews = new List<Review>(),
                });
            await col.InsertManyAsync(docs);

            await col.Indexes.CreateOneAsync(Builders<Product>.IndexKeys.Ascending(x => x.SKU), new CreateIndexOptions() { Unique = true });
            await col.Indexes.CreateOneAsync(Builders<Product>.IndexKeys.Descending(x => x.Price));
            await col.Indexes.CreateOneAsync(Builders<Product>.IndexKeys.Ascending(x => x.Description));
            await col.Indexes.CreateOneAsync(Builders<Product>.IndexKeys.Ascending(x => x.Category).Ascending(x => x.Brand));
            await col.Indexes.CreateOneAsync("{'reviews.author':1}");
        }

        public class Product
        {
            [BsonId]
            public int Id { get; set; }
            [BsonElement("sku")]
            public string SKU { get; set; }
            [BsonElement("price")]
            public double Price { get; set; }
            [BsonElement("description")]
            public string Description { get; set; }
            [BsonElement("category")]
            public string Category { get; set; }
            [BsonElement("brand")]
            public string Brand { get; set; }
            [BsonElement("reviews")]
            public List<Review> Reviews { get; set; }
            public override string ToString()
            {
                return string.Format("Description:{0}", Description);
            }
        }

        public class Review
        {
            [BsonElement("content")]
            public double Content { get; set; }
            [BsonElement("author")]
            public string Author { get; set; }
        }
    }
}
