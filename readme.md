# Contracts.py
Create a faked but coherent Redisearch database with options contracts.

# Usage
1. Create a virtual env, activate and install the requirements.
   ```
   git clone https://github.com/bjbredis/contracts.git
   virtualenv contracts
   cd contracts
   . ./bin/activate
   ```
2. Create a Redisearch DB with the following schema:
   
   `ft.create contracts schema product TAG SORTABLE expiry NUMERIC SORTABLE  delivery_class TAG SORTABLE  type TAG SORTABLE  details TEXT NOINDEX  price NUMERIC SORTABLE  qty NUMERIC SORTABLE value NUMERIC SORTABLE  market TAG SORTABLE delivery_component TEXT NOSTEM`

3. Set your environment variables: 
   
   `export REDIS_HOSTNAME=localhost; export REDIS_PORT=14000; export INDEX_NAME=contracts; export COUNT=1000;`

4. Run the script with some form of multiplicity. Example: (10x COUNT)
   
   `export END=10; for i in $(seq 1 $END); do python contracts.py \&; done`

# Sample queries
Find all contracts of a given product and delivery class:

`FT.SEARCH contracts * @delivery_class:{TSLA-DC} @product:{TSLA-P2}`

Find all delivery components of a given delivery class:

`FT.SEARCH contracts * @delivery_class:{TSLA-DC} return 1 delivery_component`

Find outstanding value of all contracts of each delivery class, sort by value descending:

`FT.AGGREGATE contracts * GROUPBY 1 @delivery_class REDUCE sum 1 @value as val SORTBY 2 @val desc`

Find outstanding aggregate value of all contracts expiring  Monday June 1 2020 for each delivery class, descending by value:

`FT.AGGREGATE contracts "@expiry:[1590978200 1590978200]" GROUPBY 1 "@delivery_class" REDUCE sum 1 @value as val SORTBY 2 @val desc`
