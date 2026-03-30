# [CREATE TABLE](https://www.cockroachlabs.com/docs/stable/create-table)
create_table:
    CREATE UNLOGGED? TABLE if_not_exist? _new_table_name (first_new_column more_new_column* table_constraint*) opt_with_table_parameter_list?

if_not_exist:
    IF NOT EXISTS

first_new_column:
    new_column

new_column_more:
    , new_column

new_column:
    _new_column_name type_name column_constraint? column_constraint?

more_new_column:
    , new_column

type_name:
    const_typename
    | const_typename
    | const_typename
    | const_typename
    | interval_type

const_typename:
    numeric
    | bit_without_length
    | bit_with_length
    | character_without_length
    | character_with_length
    | const_datetime

numeric:
    INT
    | INTEGER
    | SMALLINT
    | BIGINT
    | REAL
    | FLOAT opt_float?
    | DOUBLE PRECISION
    | DECIMAL opt_numeric_modifiers?
    | DEC opt_numeric_modifiers
    | NUMERIC opt_numeric_modifiers?
    | BOOLEAN

opt_float:
    ( _int_1_10 )

opt_numeric_modifiers:
    ( decimal_m )
    | ( decimal_m , decimal_d )

decimal_m:
    { decimal_m = math.random(1,65); print(decimal_m) }

decimal_d:
    { if decimal_m > 30 then decimal_d = math.random(0, 30) else decimal_d = math.random(0, decimal_m) end; print(decimal_d) }

bit_without_length:
    BIT VARYING?
    | VARBIT

bit_with_length:
    bit_without_length ( _int_1_10 )

character_without_length:
    char_aliases VARYING?
    | VARCHAR
    | STRING

char_aliases:
    CHAR
    | CHARACTER

character_with_length:
    character_without_length ( _int_1_10 )

const_datetime:
     DATE
     | TIME
     | time_aliases opt_timezone?
     | time_aliases ( _int_0_6 ) opt_timezone?
     | timetz_aliases
     | timetz_aliases ( _int_0_6 )

time_aliases:
    TIME
    | TIMESTAMP

timetz_aliases:
    TIMETZ
    | TIMESTAMPTZ

opt_timezone:
    WITH TIME ZONE
    | WITHOUT TIME ZONE

interval_type:
    INTERVAL
    | INTERVAL interval_qualifier

interval_qualifier:
    YEAR
    | YEAR TO MONTH
    | MONTH
    | DAY
    | DAY TO HOUR
    | DAY TO MINUTE
    | DAY TO interval_second
    | HOUR
    | HOUR TO MINUTE
    | HOUR TO interval_second
    | MINUTE
    | MINUTE TO interval_second
    | interval_second

interval_second:
    SECOND
    | SECOND ( _int_0_6 )

column_constraint:
    PRIMARY KEY
    @disable-query {PRIMARY KEY.*PRIMARY KEY}                   # Cannot add two primary keys for a table
    | NOT NULL
    @disable-query {NULL NOT NULL}                              # Cannot add not null and null constraint for a column
    | NULL
    @disable-query {NOT NULL NULL}                              # Cannot add not null and null constraint for a column
    | UNIQUE
    | check_expr
    | default_expr?
    | generated_constraint          @disable-query{c\d+\s}

check_expr:
    CHECK(bool_expr)

default_expr:
    DEFAULT (numerical_expr)

generated_constraint:
    GENERATED ALWAYS AS (numerical_expr) STORED

table_constraint:
    , table_constraint_content

table_constraint_content:
    primary_key_table_constraint
    @disable-query {PRIMARY KEY.*PRIMARY KEY}               # Cannot add two primary keys for a table
    | UNIQUE (_distinct_column more_distinct_column? more_distinct_column?)
    | FOREIGN KEY (_column) foreign_key_clause

opt_with_table_parameter_list:
    WITH ( table_parameter more_table_parameter* )

table_parameter:
    exclude_data_from_backup = _bool
    | sql_stats_automatic_collection_enabled = _bool
    | sql_stats_forecasts_enabled = _bool
    | sql_stats_automatic_collection_min_stale_rows = _int_1_100000
    | sql_stats_automatic_collection_fraction_stale_rows = _float_0_1

more_table_parameter:
    , table_parameter

primary_key_table_constraint:
    PRIMARY KEY (_distinct_column more_distinct_column? more_distinct_column?)

more_distinct_column:
    , _distinct_column

foreign_key_clause:
    REFERENCES _reference_table match_action? reference_action?

match_action:
    MATCH FULL
    | MATCH PARTIAL
    | MATCH SIMPLE

reference_action:
    ON DELETE reference_control_type
    | ON UPDATE reference_control_type

reference_control_type:
    RESTRICT
    | CASCADE
    | SET NULL
    | NO ACTION
    | SET DEFAULT

expr:
    _column
    @disable-symbol alter_table
    | bool_expr
    | numerical_expr
    | bit_expr
    | varchar_expr
    | date_expr
    | column_expr

exprs:
    , expr

column_expr:
    _column binary_arithmetic_op _column
    | arithmetic_prefix _column
    | _column binary_logical_op _column

bool_expr:
    (expr) {print("::")} BOOLEAN
    | in_expr
    | between
    | (bool_expr) binary_logical_op (bool_expr)
    | TRUE
    | FALSE
    | NOT(bool_expr)
    | ((expr) IS NULL)
    | ((expr) IS NOT NULL)
    | (numerical_expr) binary_comparison_op (numerical_expr)
    | (varchar_expr) NOT? LIKE (varchar_expr)

in_expr:
    (expr NOT? IN (expr exprs*))

between:
    (numerical_expr NOT? BETWEEN numerical_expr AND numerical_expr)

binary_logical_op:
    AND
    | OR
    | binary_equal_op

binary_comparison_op:
    <
    | <=
    | >
    | {print("=")}
    | NOT? SIMILAR TO

binary_equal_op:
    {print("=")}
    | !=

numerical_expr:
    int_expr
    | float_expr
    | double_expr
    | _digit
    | (numerical_expr) binary_arithmetic_op (numerical_expr)
    | (arithmetic_prefix(numerical_expr))
    | numerical_funcs

numerical_exprs:
    , numerical_expr

int_expr:
    _int8
    | _int8_unsigned
    | (expr) {print("::")} int_type

int_type:
    INT
    | BIGINT
    | INTEGER
    | SMALLINT
    | INT2
    | INT4
    | INT8

binary_arithmetic_op:
    {print("+")}
    | {print("-")}
    | {print("*")}
    | {print("/")}
    | {print("%")}
    | {print("^")}

float_expr:
    (expr) {print("::")} FLOAT
    | _float
    | _float_unsigned

double_expr:
    _double
    | _double_unsigned

arithmetic_prefix:
    {print("+")}
    | {print("-")}

numerical_bitwise_expr:
    (numerical_expr) binary_bitwise_op (numerical_expr)
    | {print("~")} (numerical_expr)

binary_bitwise_op:
    {print("|")}
    | {print("&")}
    | {print("<<")}
    | {print(">>")}

bit_expr:
    (expr) {print("::")} BIT
    | bit_string
    | (bit_string) {print("|")} (bit_string)
    | (bit_string) {print("&")} (bit_string)
    | {print("~")} (bit_string)
    | bitwise_funcs

bit_string:
    {print("'")}bit_value+{print("' :: BIT")}

bit_strings:
    , bit_string

bit_value:
    1
    | 0

bitwise_funcs:
    bit_length(bit_string)
    | length(bit_string)
    | octet_length(bit_string)
    | get_bit(bit_string,10)
    | set_bit(bit_string,0,bit_value)

numerical_funcs:
    abs(numerical_expr)
    | bit_length(bit_string)
    | position({print("'1'::")} BIT,bit_string)
    | get_bit(bit_string,0)
    | length(bit_string)
    | octet_length(bit_string)
    | acos(numerical_expr)
    | asin(numerical_expr)
    | atan(numerical_expr)
    | atan2(numerical_expr, numerical_expr)
    | cbrt(numerical_expr)
    | ceil(numerical_expr)
    | ceiling(numerical_expr)
    | cos(numerical_expr)
    | cot(numerical_expr)
    | degrees(numerical_expr)
    | exp(numerical_expr)
    | factorial(numerical_expr)
    | floor(numerical_expr)
    | greatest(numerical_expr numerical_exprs*)
    | isnan(numerical_expr)
    | least_common_multiple(numerical_expr, numerical_expr)
    | least(numerical_expr numerical_exprs*)
    | ln(numerical_expr)
    | log(numerical_expr)
    | pi()
    | pow(numerical_expr, numerical_expr)
    | power(numerical_expr, numerical_expr)
    | radians(numerical_expr)
    | random()    @disable-oracle equation
    | round(numerical_expr, int_expr)
    | sin(numerical_expr)
    | sign(numerical_expr)
    | sqrt(numerical_expr)
    | tan(numerical_expr)
    | @(numerical_expr)
    | date_part(date_element,date_expr)

varchar_expr:
    (expr) {print("::")} VARCHAR
    | _text
    | _char
    | collate
    | varchar_funcs

collate:
    varchar_expr collation

collation:
    COLLATE NOACCENT
    | COLLATE NOCASE
    | COLLATE C
    | COLLATE POSIX

varchar_funcs:
    strftime(date_expr, {print("'%a, %-d %B %Y'")})

date_expr:
    _date
    | _time
    | _datetime
    | _timestamp
    | (expr) {print("::")} DATE
    | CURRENT_TIME              @disable-symbol generated_constraint, create_index
                                @disable-oracle equation
    | CURRENT_DATE              @disable-symbol generated_constraint, create_index
                                @disable-oracle equation
    | CURRENT_TIMESTAMP         @disable-symbol generated_constraint, create_index
                                @disable-oracle equation
    | date_funcs
    | date_expr {print("+")} int_expr
    | date_expr {print("-")} date_expr

date_funcs:
     current_date    @disable-oracle equation
    | date_trunc(date_element, date_expr)
    | extract(date_element FROM DATE date_expr)
    | greatest(date_expr, date_expr)
    | least(date_expr, date_expr)
    | make_date(_year {print("::")} BIGINT, _month {print("::")} BIGINT, _day {print("::")} BIGINT)

date_element:
    {print("'year'")}
    | {print("'month'")}
    | {print("'day'")}
    | {print("'century'")}
    | {print("'hour'")}

# https://www.postgresql.org/docs/current/functions-conditional.html
case:
    CASE expr? WHEN expr THEN expr ELSE expr END

# https://www.postgresql.org/docs/current/sql-expressions.html#SQL-SYNTAX-TYPE-CASTS
casting:
    CAST (expr AS type_name)
    | expr {print("::")} type_name

# https://www.postgresql.org/docs/16/sql-createindex.html
# CREATE INDEX
create_index:
    CREATE UNIQUE? INDEX _new_index_name ON _table index_type? (index_table_column) where_clause?
    | CREATE UNIQUE? INDEX _new_index_name ON _table index_type? (index_table_column)

index_type:
    USING BTREE
    | USING GIST
    @disable-query {UNIQUE.*USING GIST}     # access method "giST" does not support unique indexes
    | USING GIN
    @disable-query {UNIQUE.*USING GIN}      # access method "gin" does not support unique indexes

index_table_column:
    last_indexed_column
    | last_indexed_column
    | indexed_column, last_indexed_column
    @disable-query{USING HASH.*,}           # access method "hash" does not support multicolumn

indexed_column:
    _distinct_column order_option?

order_option:
    ASC
    | DESC
    @disable-symbol last_indexed_column
    @disable-query {USING GIN.*DESC}        # access method "hash" does not support ASC/DESC options


last_indexed_column:
    indexed_column

where_clause:
    WHERE bool_expr

# https://www.postgresql.org/docs/16/sql-createview.html
# CREATE VIEW
create_view:
    CREATE VIEW if_not_exists? _new_view_name AS (simple_select)    @disable-oracle transaction_verifier
    | CREATE OR REPLACE VIEW _new_view_name AS (simple_select)      @disable-oracle transaction_verifier
    | CREATE MATERIALIZED VIEW if_not_exists? _new_view_name AS (simple_select) opt_with_data?      @disable-oracle transaction_verifier

if_not_exists:
    IF NOT EXISTS

opt_with_data:
    WITH DATA

# SHOW TABLES
show_tables:
    SHOW TABLES

# TRUNCATE TABLE
truncate:
    TRUNCATE TABLE _distinct_table more_table? cascade_or_restrict?

more_table:
    , _distinct_table

cascade_or_restrict:
    CASCADE
    | RESTRICT

# DROP TABLE
drop_table:
    DROP TABLE if_exists? _table cascade_or_restrict?

if_exists:
    IF EXISTS

# DROP INDEX
drop_index:
    DROP INDEX CONCURRENTLY? if_exists? _index RESTRICT?    # DROP INDEX CONCURRENTLY does not support CASCADE
    | DROP INDEX if_exists? _index cascade_or_restrict?
    | DROP INDEX if_exists? _index

# DROP VIEW
drop_view:
    DROP VIEW if_exists? _view cascade_or_restrict?
    | DROP MATERIALIZED VIEW if_exists? _view cascade_or_restrict?

# ALTER TABLE
alter_table:
    ALTER TABLE if_exists? _table alter_table_cmd

alter_table_cmd:
    alter_table_add_column
    | alter_table_drop_column
    | alter_table_alter_column_set_default
    | alter_table_alter_column_drop_default
    | alter_table_alter_column_set_visible
    | alter_table_alter_column_set_invisible
    | alter_table_change_column
    | alter_table_modify_column
    | alter_table_rename_column
    | alter_table_add_primary_key
    | alter_table_add_unique_key
    | alter_table_add_foreign_key
    | alter_table_rename_table
    @disable-query {, RENAME TO}

alter_table_add_column:
    ADD COLUMN? if_not_exists? new_column

alter_table_drop_column:
    ALTER TABLE _table DROP COLUMN? _drop_column

alter_table_alter_column_set_default:
    ALTER TABLE _table ALTER COLUMN? _column SET DEFAULT NULL

alter_table_alter_column_drop_default:
    ALTER TABLE _table ALTER COLUMN? _column DROP DEFAULT

alter_table_alter_column_set_visible:
    ALTER TABLE _table ALTER COLUMN? _column SET VISIBLE

alter_table_alter_column_set_invisible:
    ALTER TABLE _table ALTER COLUMN? _column SET NOT VISIBLE

alter_table_change_column:
    ALTER TABLE _table CHANGE COLUMN? _column _new_column_name type_name

alter_table_modify_column:
    ALTER TABLE _table MODIFY COLUMN? _column type_name

alter_table_rename_column:
    ALTER TABLE _table RENAME COLUMN _column TO _new_column_name

alter_table_add_primary_key:
    ALTER TABLE _table ADD PRIMARY KEY (_distinct_column more_distinct_column?)

alter_table_add_unique_key:
    ALTER TABLE _table ADD UNIQUE (_distinct_column more_distinct_column?)

alter_table_add_foreign_key:
    ALTER TABLE _table ADD FOREIGN KEY (_column) foreign_key_clause

alter_table_rename_table:
    ALTER TABLE _table RENAME to_as? _new_table_name

# INSERT
insert:
    INSERT INTO _table (_insert_columns) VALUES (_insert_values)

# UPSERT
upsert:
    UPSERT INTO _table (_insert_columns) VALUES (_insert_values)

# UPDATE
update:
    UPDATE ONLY? _table SET assignment assignment_more* where_clause?

assignment:
    _distinct_column = _value

assignment_more:
   , assignment

# DELETE
delete:
    DELETE FROM ONLY? _table where_clause?

# ANALYZE
analyze_table:
    ANALYZE _table
    | ANALYSE _table

# SET
set_variable:
    SET session_or_local? set_option

session_or_local:
    SESSION
    | LOCAL

set_option:
    {print("cost_scans_with_default_col_size")} = _bool
    | {print("index_recommendations_enabled")} = _bool
    | {print("optimizer_use_forecasts")} = _bool
    | {print("optimizer_use_histograms")} = _bool
    | {print("optimizer_use_lock_op_for_serializable")} = _bool
    | {print("optimizer_use_multicol_stats")} = _bool
    | {print("optimizer_use_not_visible_indexes")} = _bool
    | {print("prefer_lookup_joins_for_fks")} = _bool
    | {print("vectorize")} = on_or_off

on_or_off:
    ON
    | OFF

# REST
reset:
    RESET ALL
    | RESET configuration_parameter

configuration_parameter:
    {print("cost_scans_with_default_col_size")}
    | {print("disallow_full_table_scans")}
    | {print("index_recommendations_enabled")}
    | {print("optimizer_use_forecasts")}
    | {print("optimizer_use_histograms")}
    | {print("optimizer_use_lock_op_for_serializable")}
    | {print("optimizer_use_multicol_stats")}
    | {print("optimizer_use_not_visible_indexes")}
    | {print("prefer_lookup_joins_for_fks")}
    | {print("vectorize")}

# SELECT
select:
    single_select
    | single_select
    | single_select
    | ( _context single_select _end ) compound_op ( _context single_select _end )
    @disable-oracle transaction_verifier

# union type
compound_op:
    UNION all_or_distinct?
    | INTERSECT all_or_distinct?
    | EXCEPT all_or_distinct?

# Special SELECT for JOIN
single_select:
    SELECT all_or_distinct? field_list FROM join_clause
    @disable-oracle transaction_verifier
    | simple_select
    | SELECT field_list FROM join_clause
    @disable-oracle transaction_verifier
    | SELECT field_list FROM join_clause
    @disable-oracle transaction_verifier


all_or_distinct:
    ALL
    | DISTINCT

# Aliases assignment:
# JOIN ON: (1, x, 3), (x, 2, x) on_clause
# NATURAL JOIN: (x, 2, x), (1, x, 3)
join_clause:
    join_table join_type join_table on_or_use_clause    # For the INNER and OUTER join types, a join condition must be specified

join_type:
    INNER JOIN
    | LEFT OUTER JOIN
    | RIGHT OUTER JOIN
    | FULL OUTER JOIN

on_or_use_clause:
    ON bool_expr
    | USING (_using_column)
    | ON TRUE
    | ON TRUE
    | ON TRUE

join_table:
    _context (simple_select) _return AS _table_alias
    | table_source

simple_select:
    SELECT all_or_distinct? field_list FROM table_source where_clause? group_by_option? order_by_option? limit_option?
    | SELECT field_list FROM table_source where_clause?
    | SELECT field_list FROM table_source where_clause?
    | SELECT field_list FROM table_source
    | SELECT field_list FROM table_source
    | SELECT field_list FROM table_source
    | SELECT field_list FROM table_source

simple_one_select:
    SELECT _column AS _column_alias FROM table_source where_clause?
    | SELECT _column AS _column_alias FROM table_source
    | SELECT _column AS _column_alias FROM table_source

field_list:
    _column AS _column_alias,
    _column AS _column_alias,
    _column AS _column_alias

table_source:
    _table
    | _context (base_table) _return AS _table_alias
    @disable-oracle transaction_verifier

# Special select to make sure the SELECT columns are valid
base_table:
    SELECT
    _column AS _column_alias,
    _column AS _column_alias,
    _column AS _column_alias
    FROM _table

group_by_option:
    GROUP BY group_by_expr having_option?       @disable-oracle tlp_group_by, tlp_having
    | GROUP BY group_by_expr                    @enable-oracle tlp_group_by
    | GROUP BY group_by_expr having_option      @enable-oracle tlp_having

group_by_expr:
    _column, _column, _column

# HAVING expr can refer alias in select clause
# For example, SELECT c1 AS a1 FROM t1 GROUP BY c1 HAVING a1 > 10
having_option:
    HAVING bool_expr

order_by_option:
    ORDER BY _column order_option?     @disable-symbol compound_op

limit_option:
    @disable-query {\bLIMIT\b.*\b(?:IN|ALL|ANY|SOME)\b}  # MySQL 8.3.0 doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'
    LIMIT _int32_unsigned offset?

simple_select_without_temp:
    SELECT all_or_distinct? field_list FROM table_source_without_temp where_clause? group_by_option? order_by_option? limit_option?
    | SELECT field_list FROM table_source_without_temp where_clause?
    | SELECT field_list FROM table_source_without_temp where_clause?
    | SELECT field_list FROM table_source_without_temp
    | SELECT field_list FROM table_source_without_temp
    | SELECT field_list FROM table_source_without_temp
    | SELECT field_list FROM table_source_without_temp

table_source_without_temp:
    _table_without_temp
    | _context (base_table_without_temp) _return AS _table_alias
    @disable-oracle transaction_verifier

base_table_without_temp:
    SELECT
    _column AS _column_alias,
    _column AS _column_alias,
    _column AS _column_alias
    FROM _table_without_temp

# BEGIN TRANSACTION
begin:
    BEGIN
    | BEGIN transaction_characteristic
    @disable-oracle transaction_verifier
    | START TRANSACTION
    | START TRANSACTION transaction_characteristic
    @disable-oracle write_guard, transaction_verifier

transaction_characteristic:
    ISOLATION LEVEL isolation_level
    | READ WRITE
    | READ ONLY
#    | NOT? DEFERRABLE # not implemented

isolation_level:
    SERIALIZABLE
    | REPEATABLE READ
    | READ COMMITTED
    | READ UNCOMMITTED

# ROLLBACK
rollback:
    ROLLBACK

# COMMIT
commit:
    COMMIT TRANSACTION?