## Historized dbt models for `jaffle_shop`

`jaffle_shop` is a fictional ecommerce store. This [dbt](https://www.getdbt.com/) (data build tool)
project transforms raw data from an app database into an orders model ready for analytics.

This project is has an even more simplified model than the project [jaffle_shop](https://github.com/fishtown-analytics/jaffle_shop)
from which it was inspired by focusing only on status changes that happen on the orders.

An order has one of the statuses described in the [order status documentation](models/marts/core/docs.md) : 

- placed
- shipped
- completed
- return_pending
- returned

The staging table contains the following columns:

- load_id: monotonically increasing sequence for the order changes
- order_id: the order identifier
- updated_at: the date when the order was updated
- status: completion status of the order


This project offers a proof of concept on how to answer the question:

> What is the amount of orders that were being shipped in the month January?

**NOTE** that a shipment can take multiple days until the package reaches the customer and the order is marked as `completed`.

A naive answer would be to count the distinct orders from the staging table where status `shipped` is appearing.

```sql
SELECT COUNT(DISTINCT order_id)
FROM jaffle_shop.stg_orders
WHERE updated_at BETWEEN '2018-01-01' AND '2018-01-31'
AND status = 'shipped';
```

In this case the orders that started being shipped before the month of January and completed during or after this month
would not be taken into account.

One possible solution in order to the question previously mentioned would be to historize the status changes performed on the
orders which would allow to easily find the shipment date ranges that overlap with the month on which the number or orders
in shipment needs to be calculated:

```sql
-- number of orders in shipment during the month of January 2018
SELECT COUNT(DISTINCT(order_id))
FROM jaffle_shop.fct_orders
WHERE valid_from < '2018-02-01' AND valid_to >= '2018-01-01'
AND status = 'shipped';

-- number of orders in shipment during the month of February 2018
SELECT COUNT(DISTINCT(order_id))
FROM jaffle_shop.fct_orders
WHERE valid_from < '2018-03-01' AND valid_to >= '2018-02-01'
AND status = 'shipped';
```

This project provides a proof of concept on how to historize order status changes with [dbt](https://www.getdbt.com/)
models on [Snowflake](https://www.snowflake.com/) database. 

## Getting started with dbt

The [jaffle_shop](https://github.com/fishtown-analytics/jaffle_shop)
project is a useful minimum viable dbt project to get new [dbt](https://www.getdbt.com/) users 
up and running with their first dbt project. It includes [seed](https://docs.getdbt.com/docs/building-a-dbt-project/seeds)
files with generated data so that a user can run this project on their own warehouse.

---
For more information on dbt:

* Read the [introduction to dbt](https://docs.getdbt.com/docs/introduction).
* Read the [dbt viewpoint](https://docs.getdbt.com/docs/about/viewpoint).
---

## Demo

Use [virtualenv](https://pypi.org/project/virtualenv/) for creating a `virtual` python environment:

```bash
pip3 install virtualenv
virtualenv venv
source venv/bin/activate
```

Once virtualenv is set, proceed to install the requirements for the project:

```bash
(venv) ➜ pip3 install -r requirements.txt
```

Place in `~/.dbt/profiles.yml` file the following content for interacting via dbt with [Snowflake](https://www.snowflake.com/) database:
**NOTE** be sure to change the coordinates of the database according to your Snowflake account. 

```
# For more information on how to configure this file, please see:
# https://docs.getdbt.com/docs/profile
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your-account.your-snowflake-region
      port: 443
      user: "your-username"
      password: "your-password"
      role: accountadmin
      threads: 4
      database: playground
      warehouse: analysis_wh_xs
      schema: jaffle_shop
config:
  send_anonymous_usage_stats: False
```

If everything is setup correctly, dbt can be used to seed the database with test data and also to fill the models:

```bash
(venv) ➜ dbt seed --profile jaffle_shop
(venv) ➜ dbt run  --profile jaffle_shop
```

By using the Snowflake Query Browser, the number of orders in shipment during the month of _January 2018_ can be now retrieved
by executing the following sql query:

```sql
-- number of orders in shipment during the month of January 2018
SELECT COUNT(DISTINCT(order_id))
FROM jaffle_shop.fct_orders
WHERE valid_from < '2018-02-01' AND valid_to >= '2018-01-01'
AND status = 'shipped';
```

Deactivate the Python virtual environment

```bash
(venv) ➜ deactivate
```

## dbt historized macro

After running the demo, if the reader is interested to see what `dbt` is doing in the background, the content
of the logs (`logs/dbt.log`) can be investigated to see how the historization is actually performed.
The SQL queries needed to achieve the historization functionality can be consulted by checking the file
[historization-queries.sql](docs/historization-queries.sql). 

In a nutshell, the new staging entries containing order status changes get deduplicated and they get
appended to the existing history log of order status changes.

dbt offers the possibility of using [macros](https://docs.getdbt.com/docs/building-a-dbt-project/jinja-macros/#macros).
Macros are pieces of code that can be reused multiple times - same as _functions_ in other programming languages.

By means of using a specialized _macro_ for the historization functionality the complexity of building the historization
queries (see [historization-queries.sql](docs/historization-queries.sql)) is fully abstracted and the readability of the
query used for historizing the status changes is greatly simplified:  

```sql
{{
    config(
        materialized = 'historized',
        primary_key_column_name = 'order_id',
        valid_from_column_name = 'updated_at',
        load_id_column_name = 'load_id'
    )
}}

select
        load_id,
        order_id,
        updated_at,
        status
from {{ ref('stg_orders') }}

```

The implementation details for the `historized` dbt macro can be found in [historized.sql](macros/historized.sql) file.

As can be seen in the code snippet above, same as a function, the `historized` macro takes a few parameters:

- `primary_key_column_name`: the name of the column used to identify the staged entity
- `valid_from_column_name`: the name of the column containing the timestamp when the staged entity has been last updated.
- `load_id_column_name`: the name of the column containing monotonically increasing sequence values used for distinguishing the precedence
between order status changes that happen within the same timestamp.


The workflow performed within the workflow:

- create a temporary table (suffixed by `__tmp`) which contains the staged entries along with `md5_hash` column containing hash of the concatenated columns used for 
historization (e.g. : `status` in case of this example, but nothing speaks agains historizing more columns on an entity). 
The hash column can be used to easily distinguish whether two adjacent entries (ordered by load id) are 
duplicated (have the same values for the versioned columns). 
For more information on hashing, read on [The most underutilized function in SQL](https://blog.getdbt.com/the-most-underutilized-function-in-sql/) dbt blog post. 
- in case whether there are new entries staged (from the `__tmp` suffixed source table) that correspond to an 
unbounded historized entity (`valid_to` column is `NULL`) in the target table, then set the upper bound column `valid_to`
to the value corresponding to the minimum `valid_from` value of the staged entries corresponding on the target entity
- deduplicate the staged entries (based on the `md5_hash` column)
- obtain the validity range for each of the staged records (`valid_from`, `valid_to`)
- join the staged records with the records from the target table and filter out eventual duplicates (based on `md5_hash`)
- insert the staged records in the target table.



---
For more information on dbt macros:

* Read the [introduction to Jinja macros](https://docs.getdbt.com/docs/building-a-dbt-project/jinja-macros/#macros).
* Read about the [dbt materializations](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations/)
and their corresponding implementation on [Github](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations/).
---


### Ensuring accuracy of the macro via dtspec 

It takes quite a while to develop a `dbt` macro and afterwards it needs to be tested whether it works.
It may even happen that while it is productively used, a bugfix needs to be done or that the functionality of
the macro needs to be extended.
By means of using automated tests for data transformations there could be ensured that the macro works
as expected with an extensive battery of tests on a test environment.

[dtspec](https://github.com/inside-track/dtspec) is an open-source framework written in Python which can be used 
for specifying and testing data transformations.

Within `dtspec` is specified in a [yaml](https://en.wikipedia.org/wiki/YAML) format:

- the source data in the table(s) to be used by the data transformation(`dbt`) 
- the expected data from the target table(s).

`dtspec` framework offers means to read the yaml specification, and match the data from the actual tables, once
the data transformation has been performed (via `dbt`) with the data from the specification scenario.  

This project has introduced minor changes to the test code present in the project 
[jaffle_shop-dtspec](https://github.com/gnilrets/jaffle_shop-dtspec/tree/dtspec)
in order to allow it to run against [Snowflake](https://www.snowflake.com/) database.

In order to run the tests simply execute:

```bash
(venv) ➜  python tests/test.py
```

Below is presented a snippet of the output used for running the tests:

```
Executing test specification tests/demo-spec.yml
Truncating data from the tables ['raw_orders', 'fct_orders']
Inserting input data into the source table raw_orders
/home/findinpath/dbt_jaffle_shop_historized/tests/..
Running with dbt=0.18.1
Found 2 models, 11 tests, 0 snapshots, 0 analyses, 148 macros, 0 operations, 1 seed file, 0 sources

21:51:29 | Concurrency: 4 threads (target='dev')
21:51:29 | 
21:51:29 | 1 of 2 START table model jaffle_shop.stg_orders...................... [RUN]
21:51:31 | 1 of 2 OK created table model jaffle_shop.stg_orders................. [SUCCESS 1 in 2.02s]
21:51:31 | 2 of 2 START historized model jaffle_shop.fct_orders................. [RUN]
21:51:35 | 2 of 2 OK created historized model jaffle_shop.fct_orders............ [SUCCESS 1 in 4.39s]
21:51:36 | 
21:51:36 | Finished running 1 table model, 1 historized model in 10.58s.

Completed successfully

Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
Loading data from the target table fct_orders
Loading actuals for target fct_orders
Asserting Building fct_orders out of the raw_orders when the fct_orders table is empty: Basic full refresh loading of the historized fact table `fct_orders`
Tested scenarios: - order1 : the expected historized entries should follow the representation: placed > shipped > completed - order2 : the expected historized entries should follow the representation: placed - order3 : contains duplicated (with the same `updated_at` and `status` values) entries. The duplicates should be filtered out from the processing. The expected historized entries should follow the representation: placed > shipped > return_pending
 PASSED

```

**NOTE**: Make sure to follow before the steps described in the _Demo_ section of this document.

To give to the reader a hint about what happens when dtspec finds a mismatch when verifying the content of the 
target table against what is present in the specification, there is presented also a snippet of the output of a failing test:

```
DataFrame.iloc[:, 2] (column name="status") values are different (25.0 %)
[index]: [0, 1, 2, 3]
[left]:  [placed, completed, shipped, placed]
[right]: [returned, completed, shipped, placed]
Actual:
  order_id load_id     status  valid_from    valid_to
0   order1       1     placed  2018-01-01  2018-01-03
1   order1      11  completed  2018-01-04      {NULL}
2   order1       3    shipped  2018-01-03  2018-01-04
3   order2      10     placed  2018-01-02      {NULL}
Expected:
  order_id load_id     status  valid_from    valid_to
0   order1       1   returned  2018-01-01  2018-01-03
1   order1      11  completed  2018-01-04      {NULL}
2   order1       3    shipped  2018-01-03  2018-01-04
3   order2      10     placed  2018-01-02      {NULL}
```

---
For more information on dtspec:

* Visit the [dtspec](https://github.com/inside-track/dtspec) Github project page
* Visit the [jaffle_shop-dtspec](https://github.com/gnilrets/jaffle_shop-dtspec/tree/dtspec) Github project to get an
introduction on how to work with `dtspec` & `dbt` for the [jaffle-shop](https://github.com/fishtown-analytics/jaffle_shop)
dbt tutorial project.
---

## Conclusion

This proof of concept project compiles together several topics:

- dbt macros
- data transformation specification
- historization of entities

into a functioning prototype for historizing order status changes.
Feel free to provide feedback or alternative implementations to any of the topics presented in this project. 