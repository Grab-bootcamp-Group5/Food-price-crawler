use metadata_db_v2

db.category_shards.insertMany([
  {
  Category: "Beverages",
  db_name: "products_beverages",
  collection_name: "beverages_v2",
  server_uri: "mongodb://localhost:27017"
  },
    {
  Category: "Alcoholic Beverages",
  db_name: "products_alcoholic_beverages",
  collection_name: "alcoholic_beverages_v2",
  server_uri: "mongodb://localhost:27017"
    },
{
  Category: "Cakes",
  db_name: "products_cakes",
  collection_name: "cakes_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Candies",
  db_name: "products_candies",
  collection_name: "candies_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Cereals & Grains",
  db_name: "products_cereals_and_grains",
  collection_name: "cereals_and_grains_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Cold Cuts: Sausages & Ham",
  db_name: "products_cold_cuts_sausages_and_ham",
  collection_name: "cold_cuts_sausages_and_ham_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Dried Fruits",
  db_name: "products_dried_fruits",
  collection_name: "dried_fruits_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Fresh Fruits",
  db_name: "products_fresh_fruits",
  collection_name: "fresh_fruits_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Fresh Meat",
  db_name: "products_fresh_meat",
  collection_name: "fresh_meat_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Fruit Jam",
  db_name: "products_fruit_jam",
  collection_name: "fruit_jam_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Grains & Staples",
  db_name: "products_grains_and_staples",
  collection_name: "grains_and_staples_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Ice Cream & Cheese",
  db_name: "products_ice_cream_and_cheese",
  collection_name: "ice_cream_and_cheese_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Instant Foods",
  db_name: "products_instant_foods",
  collection_name: "instant_foods_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Milk",
  db_name: "products_milk",
  collection_name: "milk_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Prepared Vegetables",
  db_name: "products_prepared_vegetables",
  collection_name: "prepared_vegetables_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Seafood & Fish Balls",
  db_name: "products_seafood_and_fish_balls",
  collection_name: "seafood_and_fish_balls_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Seasonings",
  db_name: "products_seasonings",
  collection_name: "seasonings_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Snacks",
  db_name: "products_snacks",
  collection_name: "snacks_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Vegetables",
  db_name: "products_vegetables",
  collection_name: "vegetables_v2",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Yogurt",
  db_name: "products_yogurt",
  collection_name: "yogurt_v2",
  server_uri: "mongodb://localhost:27017"
}]);


dbs = [
  { db: "products_beverages", collection: "beverages_v2" },
  { db: "products_cakes", collection: "cakes_v2" },
  { db: "products_candies", collection: "candies_v2" },
  { db: "products_cereals_and_grains", collection: "cereals_and_grains_v2" },
  { db: "products_cold_cuts_sausages_and_ham", collection: "cold_cuts_sausages_and_ham_v2" },
  { db: "products_dried_fruits", collection: "dried_fruits_v2" },
  { db: "products_fresh_fruits", collection: "fresh_fruits_v2" },
  { db: "products_fresh_meat", collection: "fresh_meat_v2" },
  { db: "products_fruit_jam", collection: "fruit_jam_v2" },
  { db: "products_grains_and_staples", collection: "grains_and_staples_v2" },
  { db: "products_ice_cream_and_cheese", collection: "ice_cream_and_cheese_v2" },
  { db: "products_instant_foods", collection: "instant_foods_v2" },
  { db: "products_milk", collection: "milk_v2" },
  { db: "products_prepared_vegetables", collection: "prepared_vegetables_v2" },
  { db: "products_seafood_and_fish_balls", collection: "seafood_and_fish_balls_v2" },
  { db: "products_seasonings", collection: "seasonings_v2" },
  { db: "products_snacks", collection: "snacks_v2" },
  { db: "products_vegetables", collection: "vegetables_v2" },
  { db: "products_yogurt", collection: "yogurt_v2" },
  { db: "products_alcoholic_beverages", collection: "alcoholic_beverages_v2" }
]

dbs.forEach(item => {
  const database = db.getSiblingDB(item.db);
  database.createCollection(item.collection);
  print("Created collection: " + item.db + "." + item.collection);

  database.getCollection(item.collection).createIndex(
    { name: "text" }
  );
  print("Created text index on: " + item.db + "." + item.collection);
});
