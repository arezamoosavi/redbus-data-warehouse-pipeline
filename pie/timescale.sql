create table if not exists tr_data
(
    created timestamp NOT NULL DEFAULT NOW(),
    source text NOT NULL,
    target text NOT NULL,
    amount numeric NOT NULL,
    currency VARCHAR (10) NOT NULL,
    tag VARCHAR (10) NOT NULL
);

SELECT * FROM timescaledb_information.data_nodes;
SELECT * FROM timescaledb_information.hypertables;
SELECT * FROM timescaledb_information.dimensions;

SELECT create_distributed_hypertable('tr_data', 'created', 'source',
    data_nodes => '{ "timescaledb-data1", "timescaledb-data2" }');

alter table tr_data set (
  timescaledb.compress,
  timescaledb.compress_orderby = 'created');


select date(created), tag, amount,
      avg(amount) OVER(
   PARTITION BY tag) as avg_amount,
      count(tag) OVER(
   PARTITION BY tag) as tag_count
from tr_data
where amount > 600;
