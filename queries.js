const { MongoClient } = require('mongodb');

const uri = 'mongodb://localhost:27017';
const dbName = 'plp_bookstore';
const collectionName = 'books';

async function task1BasicQueries(collection) {
  const allBooks = await collection.find().toArray();
  console.log("\nTask 1: All books:", allBooks.length);
}

async function task2Filtering(collection) {
  const fantasyBooks = await collection.find({ genre: 'Fantasy' }).toArray();
  console.log("\nTask 2a: Fantasy books:", fantasyBooks);

  const booksAfter2000 = await collection.find({ published_year: { $gt: 2000 } }).toArray();
  console.log("\nTask 2b: Books published after 2000:", booksAfter2000);

  const orwellBooks = await collection.find({ author: 'George Orwell' }).toArray();
  console.log("\nTask 2c: Books by George Orwell:", orwellBooks);

  const updateResult = await collection.updateOne(
    { title: 'The Great Gatsby' },
    { $set: { price: 15.99 } }
  );
  console.log("\nTask 2d: Update result:", updateResult.modifiedCount);

  const deleteResult = await collection.deleteOne({ title: 'Wuthering Heights' });
  console.log("\nTask 2e: Delete result:", deleteResult.deletedCount);
}

async function task3AdvancedQueries(collection) {
  const booksInStockAfter2010 = await collection.find({
    in_stock: true,
    published_year: { $gt: 2010 }
  }).project({ title: 1, author: 1, price: 1, _id: 0 }).toArray();
  console.log("\nTask 3a: In-stock books after 2010:", booksInStockAfter2010);

  const sortedAsc = await collection.find().sort({ price: 1 }).toArray();
  console.log("\nTask 3b: Books sorted by price ASC:", sortedAsc.map(b => b.title));

  const sortedDesc = await collection.find().sort({ price: -1 }).toArray();
  console.log("\nTask 3c: Books sorted by price DESC:", sortedDesc.map(b => b.title));

  const page1 = await collection.find().skip(0).limit(5).toArray();
  console.log("\nTask 3d: Page 1:", page1.map(b => b.title));

  const page2 = await collection.find().skip(5).limit(5).toArray();
  console.log("Task 3d: Page 2:", page2.map(b => b.title));
}

async function task4Aggregations(collection) {
  const avgPrice = await collection.aggregate([
    { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } },
    { $sort: { avgPrice: -1 } }
  ]).toArray();
  console.log("\nTask 4a: Avg price by genre:", avgPrice);

  const topAuthor = await collection.aggregate([
    { $group: { _id: "$author", count: { $sum: 1 } } },
    { $sort: { count: -1 } },
    { $limit: 1 }
  ]).toArray();
  console.log("\nTask 4b: Author with most books:", topAuthor);

  const byDecade = await collection.aggregate([
    {
      $project: {
        decade: {
          $concat: [
            { $toString: { $subtract: ["$published_year", { $mod: ["$published_year", 10] }] } },
            "s"
          ]
        }
      }
    },
    {
      $group: {
        _id: "$decade",
        count: { $sum: 1 }
      }
    },
    { $sort: { _id: 1 } }
  ]).toArray();
  console.log("\nTask 4c: Books by decade:", byDecade);
}

async function run() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    console.log('Connected to MongoDB');
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    await task1BasicQueries(collection);
    await task2Filtering(collection);
    await task3AdvancedQueries(collection);
    await task4Aggregations(collection);
    await task5Indexing(collection);


  } catch (error) {
    console.error(error);
  } finally {
    await client.close();
    console.log("Connection closed");
  }
}

run();

async function task5Indexing(collection) {
    // Create an index on the title field
    await collection.createIndex({ title: 1 });
    console.log("\nTask 5a: Created index on 'title'");
  
    // Create a compound index on author and published_year
    await collection.createIndex({ author: 1, published_year: 1 });
    console.log("Task 5b: Created compound index on 'author' and 'published_year'");
  
    // Use explain() to demonstrate performance benefit
    const explainResult = await collection.find({ title: "1984" }).explain("executionStats");
    console.log("Task 5c: Explain output for 'title' query:");
    console.dir(explainResult.executionStats, { depth: null });
  }
  