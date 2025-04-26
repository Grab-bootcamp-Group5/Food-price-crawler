use metadata_db

db.createCollection("category_shards")

db.category_shards.insertMany([
  {
  Category: "Beverages",
  db_name: "products_beverages",
  collection_name: "beverages",
  server_uri: "mongodb://localhost:27017"
  },
    {
  Category: "Alcoholic Beverages",
  db_name: "products_alcoholic_beverages",
  collection_name: "alcoholic_beverages",
  server_uri: "mongodb://localhost:27017"
    },
{
  Category: "Cakes",
  db_name: "products_cakes",
  collection_name: "cakes",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Candies",
  db_name: "products_candies",
  collection_name: "candies",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Cereals & Grains",
  db_name: "products_cereals_and_grains",
  collection_name: "cereals_and_grains",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Cold Cuts: Sausages & Ham",
  db_name: "products_cold_cuts_sausages_and_ham",
  collection_name: "cold_cuts_sausages_and_ham",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Dried Fruits",
  db_name: "products_dried_fruits",
  collection_name: "dried_fruits",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Fresh Fruits",
  db_name: "products_fresh_fruits",
  collection_name: "fresh_fruits",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Fresh Meat",
  db_name: "products_fresh_meat",
  collection_name: "fresh_meat",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Fruit Jam",
  db_name: "products_fruit_jam",
  collection_name: "fruit_jam",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Grains & Staples",
  db_name: "products_grains_and_staples",
  collection_name: "grains_and_staples",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Ice Cream & Cheese",
  db_name: "products_ice_cream_and_cheese",
  collection_name: "ice_cream_and_cheese",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Instant Foods",
  db_name: "products_instant_foods",
  collection_name: "instant_foods",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Milk",
  db_name: "products_milk",
  collection_name: "milk",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Prepared Vegetables",
  db_name: "products_prepared_vegetables",
  collection_name: "prepared_vegetables",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Seafood & Fish Balls",
  db_name: "products_seafood_and_fish_balls",
  collection_name: "seafood_and_fish_balls",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Seasonings",
  db_name: "products_seasonings",
  collection_name: "seasonings",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Snacks",
  db_name: "products_snacks",
  collection_name: "snacks",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Vegetables",
  db_name: "products_vegetables",
  collection_name: "vegetables",
  server_uri: "mongodb://localhost:27017"
},
{
  Category: "Yogurt",
  db_name: "products_yogurt",
  collection_name: "yogurt",
  server_uri: "mongodb://localhost:27017"
}]);


dbs = [
  { db: "products_beverages", collection: "beverages" },
  { db: "products_cakes", collection: "cakes" },
  { db: "products_candies", collection: "candies" },
  { db: "products_cereals_and_grains", collection: "cereals_and_grains" },
  { db: "products_cold_cuts_sausages_and_ham", collection: "cold_cuts_sausages_and_ham" },
  { db: "products_dried_fruits", collection: "dried_fruits" },
  { db: "products_fresh_fruits", collection: "fresh_fruits" },
  { db: "products_fresh_meat", collection: "fresh_meat" },
  { db: "products_fruit_jam", collection: "fruit_jam" },
  { db: "products_grains_and_staples", collection: "grains_and_staples" },
  { db: "products_ice_cream_and_cheese", collection: "ice_cream_and_cheese" },
  { db: "products_instant_foods", collection: "instant_foods" },
  { db: "products_milk", collection: "milk" },
  { db: "products_prepared_vegetables", collection: "prepared_vegetables" },
  { db: "products_seafood_and_fish_balls", collection: "seafood_and_fish_balls" },
  { db: "products_seasonings", collection: "seasonings" },
  { db: "products_snacks", collection: "snacks" },
  { db: "products_vegetables", collection: "vegetables" },
  { db: "products_yogurt", collection: "yogurt" },
  { db: "products_alcoholic_beverages", collection: "alcoholic_beverages" }
]

dbs.forEach(item => {
  db.createCollection(item.collection)
  print("Created: " + item.db + "." + item.collection)
})
