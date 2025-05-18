-- /Users/will/workspace/presto/presto-cli/target/presto-cli-*-executable.jar --server localhost:8080 -f prepare.sql

CREATE SCHEMA memory.tiny;

CREATE TABLE memory.tiny.orders (
    orderkey bigint,
    custkey bigint,
    orderstatus varchar(1),
    totalprice double,
    orderdate date,
    orderpriority varchar(15),
    clerk varchar(15),
    shippriority integer,
    comment varchar(79)
 );

 CREATE TABLE memory.tiny.lineitem (
    orderkey bigint,
    partkey bigint,
    suppkey bigint,
    linenumber integer,
    quantity double,
    extendedprice double,
    discount double,
    tax double,
    returnflag varchar(1),
    linestatus varchar(1),
    shipdate date,
    commitdate date,
    receiptdate date,
    shipinstruct varchar(25),
    shipmode varchar(10),
    comment varchar(44)
 );

CREATE TABLE memory.tiny."bucketed-orders" (
    orderkey bigint,
    custkey bigint,
    orderstatus varchar(1),
    totalprice double,
    orderdate date,
    orderpriority varchar(15),
    clerk varchar(15),
    shippriority integer,
    comment varchar(79)
 ) with ("bucketed-by" = 'orderkey');

CREATE TABLE memory.tiny."bucketed-lineitem" (
    orderkey bigint,
    partkey bigint,
    suppkey bigint,
    linenumber integer,
    quantity double,
    extendedprice double,
    discount double,
    tax double,
    returnflag varchar(1),
    linestatus varchar(1),
    shipdate date,
    commitdate date,
    receiptdate date,
    shipinstruct varchar(25),
    shipmode varchar(10),
    comment varchar(44)
 ) with ("bucketed-by" = 'orderkey');

--INSERT INTO memory.tiny.orders (SELECT * FROM tpch.tiny.orders);
--select count(*) from memory.tiny.orders;
--
--INSERT INTO memory.tiny.lineitem (SELECT * FROM tpch.tiny.lineitem);
--select count(*) from memory.tiny.lineitem;
--
--INSERT INTO memory.tiny."bucketed-orders" (SELECT * FROM tpch.tiny.orders);
--select count(*) from memory.tiny."bucketed-orders";
--
--INSERT INTO memory.tiny."bucketed-lineitem" (SELECT * FROM tpch.tiny.lineitem);
--select count(*) from memory.tiny."bucketed-lineitem";

CREATE TABLE memory.tiny.complex_types_table (
    id INT,
    user_info ROW(
        name VARCHAR,
        age INT,
        address ROW(city VARCHAR, zip_code VARCHAR)
    ),
    interests ARRAY<VARCHAR>,
    preferences MAP<VARCHAR, VARCHAR>
);

INSERT INTO memory.tiny.complex_types_table (id, user_info, interests, preferences) VALUES
(1,
 ROW('Alice', 30, ROW('New York', '10001')),
 ARRAY['reading', 'music'],
 MAP(ARRAY['theme', 'language'], ARRAY['dark', 'en'])
),
(2,
 ROW('Bob', 25, ROW('London', 'SW1A 1AA')),
 ARRAY['sports', 'coding'],
 MAP(ARRAY['theme', 'language'], ARRAY['light', 'fr'])
);

set session pushdown_subfields_enabled=true;
SELECT
    id,
    user_info.name,
    user_info.address.city,
    interests[1] AS first_interest,
    preferences['theme'] AS theme
FROM memory.tiny.complex_types_table;