# CREATE TABLE
create_table:
    CREATE or_replace? TABLE if_not_exists? _new_table_name (first_new_column more_new_column* table_constraint*) table_option*
    # | CREATE TEMPORARY? TABLE if_not_exists? _new_table_name (first_new_column more_new_column* table_constraint*) table_option*
    # | CREATE TEMPORARY? TABLE if_not_exists? _new_table_name LIKE _table  # copy create table
    # | CREATE TEMPORARY? TABLE if_not_exists? _new_table_name AS? select # query create table

or_replace:
    OR REPLACE

if_not_exists:
    IF NOT EXISTS    @disable-query {((?:OR\s+REPLACE\s+)?\w+(?:IF\s+NOT\s+EXISTS\s+)?\w+)}

first_new_column:
    new_column

new_column:
    _new_column_name type_name column_constraint?

more_new_column:
    , new_column

type_name:
    string_data_type charset_name?
    | blob_data_type
    | int_data_type SIGNED?
    | int_data_type UNSIGNED? ZEROFILL?
    | dec_data_type length_two_dimension? SIGNED?
    | dec_data_type length_two_dimension? UNSIGNED? ZEROFILL?
    | float_data_type
    | BIT length_bit?

length_one_dimension:
    ( _int4_unsigned )

length_two_dimension:
    ( decimal_m , decimal_d ) # Maximum is 65 for DECIMAL

decimal_m:
    { decimal_m = math.random(1,65); print(decimal_m) }

decimal_d:
    { if decimal_m > 30 then decimal_d = math.random(0, 30) else decimal_d = math.random(0, decimal_m) end; print(decimal_d) }

length_bit:
    ( { length_bit = math.random(1,64); print(length_bit) } )

charset_name: # not complete
    CHARACTER SET BINARY
    | CHARACTER SET ASCII
    | CHARACTER SET UTF8

string_data_type:
    CHAR
    | CHARACTER
    | VARCHAR length_one_dimension
    | TINYTEXT
    | TEXT length_one_dimension?
    | MEDIUMTEXT
    | LONGTEXT

blob_data_type:
    BLOB length_one_dimension?
    | BINARY length_one_dimension?
    | VARBINARY length_one_dimension
    | TINYBLOB
    | MEDIUMBLOB
    | LONGBLOB

int_data_type:
    TINYINT
    | SMALLINT
    | MEDIUMINT
    | INT
    | INTEGER
    | BIGINT
    | INT1
    | INT2
    | INT3
    | INT4
    | INT8

dec_data_type:
    DECIMAL
    | NUMERIC

float_data_type:
    FLOAT
    | FLOAT4
    | FLOAT8
    | DOUBLE

column_constraint:
    PRIMARY KEY
    @disable-query {PRIMARY KEY.*PRIMARY KEY}                       # Cannot add two primary keys for a table
    @disable-query {(BLOB|TEXT)(?!.*,.*PRIMARY KEY).*PRIMARY KEY}   # BLOB/TEXT column requires a key length
    | NOT NULL
    | UNIQUE
    @disable-query {(BLOB|TEXT)(?!.*,.*UNIQUE).*UNIQUE}             # BLOB/TEXT column requires a key length
    | NULL

table_option:
    AUTO_INCREMENT = _int32_unsigned
    | AVG_ROW_LENGTH = _int32_unsigned
    | CHECKSUM = two_value_option
    | DELAY_KEY_WRITE = two_value_option
    | ENGINE = engine_option                                        @disable-oracle write_guard
    | INSERT_METHOD = insert_method_option
    | MAX_ROWS = _int32_unsigned
    | MIN_ROWS = _int32_unsigned
    | PACK_KEYS = three_value_option
    | STATS_AUTO_RECALC = three_value_option
    | STATS_PERSISTENT = three_value_option
    | STATS_SAMPLE_PAGES = _int8_unsigned

two_value_option:
    0
    | 1

engine_option:
    InnoDB
    | InnoDB
    | InnoDB
    | InnoDB
    | InnoDB
    | MyISAM
    @disable-oracle transaction_verifier, write_guard
    | MEMORY
    @disable-oracle transaction_verifier, write_guard
    | HEAP
    @disable-oracle transaction_verifier, write_guard
    | CSV
    @disable-oracle transaction_verifier, write_guard
    | ARCHIVE
    @disable-query {(?i)(PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY).*ARCHIVE}    # ARCHIVE does not support indexes
    @disable-oracle transaction_verifier, write_guard

insert_method_option:
    NO
    | FIRST
    | LAST

three_value_option:
    1
    | 0
    | DEFAULT

table_constraint:
    primary_key_table_constraint
    @disable-query {PRIMARY KEY.*PRIMARY KEY}               # Cannot add two primary keys for a table
    | , UNIQUE (_distinct_key_column more_distinct_key_column? more_distinct_key_column?)
    | , FOREIGN KEY (_fk_column) foreign_key_clause

primary_key_table_constraint:
    , PRIMARY KEY (_distinct_key_column more_distinct_key_column? more_distinct_key_column?)

more_distinct_key_column:
    , _distinct_key_column

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

# DROP TABLE
drop_table:
    DROP TABLE if_exists? _table cascade_or_restrict?
    | DROP TEMPORARY? TABLE if_exists? _temp_table cascade_or_restrict?
    | DROP TABLE if_exists? _table cascade_or_restrict?
    | DROP TABLE if_exists? _table cascade_or_restrict?

if_exists:
    IF EXISTS

cascade_or_restrict:
    RESTRICT
    | CASCADE

# ALTER TABLE
alter_table:
    alter_table_add_column
    | alter_table_drop_column
    | alter_table_alter_column_set_default
    | alter_table_alter_column_drop_default
    | alter_table_alter_column_set_visible
    | alter_table_alter_column_set_invisible
    | alter_table_change_column
    | alter_table_modify_column
    | alter_table_rename_column
    | alter_table_add_index
    | alter_table_drop_index
    | alter_table_rename_index
    | alter_table_add_primary_key
    | alter_table_drop_primary_key
    | alter_table_add_unique_key
    | alter_table_add_foreign_key
    | alter_table_rename_table
    | alter_table_option

algorithm:
    , ALGORITHM COPY
    | , ALGORITHM DEFAULT

alter_table_add_column:
    ALTER TABLE _table ADD COLUMN? _new_column_name type_name algorithm?
    | ALTER TABLE _table ADD COLUMN? _new_column_name type_name FIRST algorithm?
    | ALTER TABLE _table ADD COLUMN? _new_column_name type_name AFTER _column algorithm?

alter_table_drop_column:
    ALTER TABLE _table DROP COLUMN? _drop_column algorithm?

alter_table_alter_column_set_default:
    ALTER TABLE _table ALTER COLUMN? _column SET DEFAULT NULL

alter_table_alter_column_set_visible:
    ALTER TABLE _table ALTER COLUMN? _column SET VISIBLE

alter_table_alter_column_drop_default:
    ALTER TABLE _table ALTER COLUMN? _column DROP DEFAULT

alter_table_alter_column_set_invisible:
    ALTER TABLE _table ALTER COLUMN? _column SET INVISIBLE

alter_table_change_column:
    ALTER TABLE _table CHANGE COLUMN? _column _new_column_name type_name algorithm?
    | ALTER TABLE _table CHANGE COLUMN? _column _new_column_name type_name FIRST algorithm?

alter_table_modify_column:
    ALTER TABLE _table MODIFY COLUMN? _column type_name algorithm?
    | ALTER TABLE _table MODIFY COLUMN? _column type_name FIRST algorithm?

alter_table_rename_column:
    ALTER TABLE _table RENAME COLUMN _column TO _new_column_name

alter_table_add_index:
    ALTER TABLE _table ADD INDEX _new_index_name index_type? (index_table_column) index_option?
    | ALTER TABLE _table ADD KEY _new_index_name index_type? (index_table_column) index_option?

alter_table_drop_index:
    ALTER TABLE _table DROP INDEX _index
    | ALTER TABLE _table DROP KEY _index

alter_table_rename_index:
    ALTER TABLE _table RENAME INDEX _index TO _new_index_name
    | ALTER TABLE _table RENAME KEY _index TO _new_index_name

alter_table_add_primary_key:
    ALTER TABLE _table ADD PRIMARY KEY (_distinct_key_column more_distinct_key_column?)

alter_table_drop_primary_key:
    ALTER TABLE _table DROP PRIMARY KEY

alter_table_add_unique_key:
    ALTER TABLE _table ADD UNIQUE (_distinct_key_column more_distinct_key_column?)
    | ALTER TABLE _table ADD UNIQUE INDEX (_distinct_key_column more_distinct_key_column?)
    | ALTER TABLE _table ADD UNIQUE KEY (_distinct_key_column more_distinct_key_column?)

alter_table_add_foreign_key:
    ALTER TABLE _table ADD FOREIGN KEY (_fk_column) foreign_key_clause

alter_table_rename_table:
    ALTER TABLE _table RENAME to_as? _new_table_name

alter_table_option:
    ALTER TABLE _table alter_checksum
    | ALTER TABLE _table delay_key_write
    | ALTER TABLE _table disable_enable_keys
    | ALTER TABLE _table FORCE
    | ALTER TABLE _table insert_method
    | ALTER TABLE _table row_format
    | ALTER TABLE _table stats_auto_recalc
    | ALTER TABLE _table stats_persistent
    | ALTER TABLE _table pack_keys

alter_checksum:
    CHECKSUM 0
    | CHECKSUM 1

delay_key_write:
    DELAY_KEY_WRITE 0
    | DELAY_KEY_WRITE 1

disable_enable_keys:
    DISABLE KEYS
    | ENABLE KEYS

insert_method:
    INSERT_METHOD NO
    | INSERT_METHOD FIRST
    | INSERT_METHOD LAST

row_format:
    ROW_FORMAT DEFAULT
    | ROW_FORMAT DYNAMIC
    | ROW_FORMAT COMPRESSED
    | ROW_FORMAT REDUNDANT
    | ROW_FORMAT COMPACT

stats_auto_recalc:
    STATS_AUTO_RECALC 0
    | STATS_AUTO_RECALC 1
    | STATS_AUTO_RECALC DEFAULT

stats_persistent:
    STATS_PERSISTENT 0
    | STATS_PERSISTENT 1
    | STATS_PERSISTENT DEFAULT

pack_keys:
    PACK_KEYS 0
    | PACK_KEYS 1
    | PACK_KEYS DEFAULT

rename_table:
    RENAME TABLE _table TO _new_table_name

to_as:
    TO
    | AS

# CREATE VIEW
create_view:
    CREATE or_replace? create_view_algorithm? VIEW if_not_exists? _new_view_name AS simple_select_without_select_spec    @disable-oracle write_guard, transaction_verifier

create_view_algorithm:
    ALGORITHM = UNDEFINED
    | ALGORITHM = MERGE
    | ALGORITHM = TEMPTABLE

# DROP VIEW
drop_view:
    DROP VIEW if_exists? _view cascade_or_restrict?

# SELECT
select:
    single_select
    | single_select
    | single_select
    | _context single_select_without_select_spec _end compound_op _context single_select_without_select_spec _end
    @disable-oracle transaction_verifier

# union type
compound_op:
    UNION
    | UNION ALL
    | INTERSECT
    | EXCEPT

# Special SELECT for JOIN
single_select:
    SELECT select_spec? field_list FROM join_clause
    @disable-oracle transaction_verifier
    | simple_select
    | SELECT field_list FROM join_clause
    @disable-oracle transaction_verifier
    | SELECT field_list FROM join_clause
    @disable-oracle transaction_verifier

single_select_without_select_spec:
    SELECT field_list FROM join_clause
    | simple_select
    | SELECT field_list FROM join_clause
    | SELECT field_list FROM join_clause

select_spec:
    all_or_distinct_or_distinct_row
    | HIGH_PRIORITY
    | STRAIGHT_JOIN
    | SQL_SMALL_RESULT
    | SQL_BIG_RESULT
    | SQL_BUFFER_RESULT
    | sql_cache_or_sql_no_cache
    | SQL_CALC_FOUND_ROWS

all_or_distinct_or_distinct_row:
    ALL
    | DISTINCT
    | DISTINCTROW

sql_cache_or_sql_no_cache:
    SQL_CACHE
    | SQL_NO_CACHE

# Aliases assignment:
# JOIN ON: (1, x, 3), (x, 2, x) on_clause
# NATURAL JOIN: (x, 2, x), (1, x, 3)
join_clause:
    join_table join_type join_table on_or_use_clause?
    | join_table NATURAL JOIN join_table
    | join_table join_type join_table
    | join_table NATURAL JOIN join_table
    | join_table join_type join_table

join_type:
    JOIN
    | CROSS JOIN
    | INNER JOIN
    | LEFT JOIN

on_or_use_clause:
    ON expr_without_aggregate
    | USING (_using_column)


join_table:
    _context (simple_select_without_select_spec) _return AS _table_alias
    | table_source

simple_select:
    SELECT select_spec? field_list FROM table_source where_clause? group_by_option? order_by_option? limit_option? for_update_lock_option?    @disable-oracle equation, write_guard, transaction_verifier
    | SELECT field_list FROM table_source where_clause?
    | SELECT field_list FROM table_source where_clause?
    | SELECT field_list FROM table_source
    | SELECT field_list FROM table_source
    @disable-oracle transaction_verifier
    | SELECT field_list FROM table_source
    @disable-oracle transaction_verifier
    | SELECT field_list FROM table_source
    @disable-oracle transaction_verifier
    | SELECT field_list FROM table_source where_clause
    @enable-oracle transaction_verifier
    | SELECT field_list FROM table_source where_clause
    @enable-oracle transaction_verifier

simple_select_without_select_spec:
    SELECT field_list FROM table_source where_clause? group_by_option? order_by_option? limit_option? for_update_lock_option?    @disable-oracle equation, write_guard, transaction_verifier
    | SELECT field_list FROM table_source where_clause? group_by_option? order_by_option? limit_option?    @disable-oracle equation
    | SELECT field_list FROM table_source where_clause?
    | SELECT field_list FROM table_source where_clause?
    | SELECT field_list FROM table_source
    | SELECT field_list FROM table_source
    | SELECT field_list FROM table_source
    | SELECT field_list FROM table_source

for_update_lock_option:
    FOR UPDATE lock_option_in_select?
    | LOCK IN SHARE MODE lock_option_in_select?

lock_option_in_select:
    WAIT _int8_unsigned
    | NOWAIT
    | SKIP LOCKED

simple_one_select:
    SELECT _column AS _column_alias FROM table_source where_clause?
    | SELECT _column AS _column_alias FROM table_source
    | SELECT _column AS _column_alias FROM table_source
    @disable-oracle transaction_verifier

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
    _column

# HAVING expr can refer alias in select clause
# For example, SELECT c1 AS a1 FROM t1 GROUP BY c1 HAVING a1 > 10
having_option:
    HAVING expr_with_aggregate

order_by_option:
    ORDER BY ordering_term more_ordering_term?

ordering_term:
    _column

more_ordering_term:
    , ordering_term

limit_option:
    @disable-query {\bLIMIT\b.*\b(?:IN|ALL|ANY|SOME)\b}  # MySQL 8.3.0 doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'
    LIMIT _int32_unsigned offset?

# UPDATE
update:
    UPDATE LOW_PRIORITY? IGNORE? _table SET assignment assignment_more* where_clause?
    | UPDATE LOW_PRIORITY? IGNORE? _table SET assignment assignment_more* where_clause
    @enable-oracle transaction_verifier
    | UPDATE LOW_PRIORITY? IGNORE? _table SET assignment assignment_more* where_clause
    @enable-oracle transaction_verifier
    | UPDATE LOW_PRIORITY? IGNORE? _table SET assignment assignment_more* where_clause? order_by_option? limit_option?    @disable-oracle equation
    | UPDATE LOW_PRIORITY? IGNORE? _distinct_table more_tables* SET assignment assignment_more* where_clause?             @disable-oracle equation, transaction_verifier

assignment:
    _distinct_column = _value

assignment_more:
   , assignment

more_tables:
   , _distinct_table

# DELETE
delete:
    DELETE LOW_PRIORITY? QUICK? IGNORE? FROM _table where_clause?
    | DELETE LOW_PRIORITY? QUICK? IGNORE? FROM _table where_clause
    @enable-oracle transaction_verifier
    | DELETE LOW_PRIORITY? QUICK? IGNORE? FROM _table where_clause
    @enable-oracle transaction_verifier
    | DELETE LOW_PRIORITY? QUICK? IGNORE? FROM _table where_clause? order_by_option? limit_option? returning_clause?

where_clause:
    WHERE expr_without_aggregate

expr_without_aggregate:
    (expr)
    @disable-oracle transaction_verifier
    | (simple_expr)

expr_with_aggregate:
    (expr)
    @disable-oracle transaction_verifier
    | (simple_expr)

# INSERT
insert:
    INSERT insert_option? IGNORE? INTO _table (_insert_columns) VALUES (_insert_values)
    | INSERT insert_option? IGNORE? INTO _table (_insert_columns) VALUES (_insert_values) conflict_option? returning_clause?    @disable-oracle equation
    | INSERT insert_option? IGNORE? INTO _table SET assignment assignment_more* conflict_option? returning_clause?              @disable-oracle equation

insert_option:
    LOW_PRIORITY
    | HIGH_PRIORITY

conflict_option:
    ON DUPLICATE KEY UPDATE _column = _value

returning_clause:
    RETURNING field_list

# REPLACE
replace:
    REPLACE LOW_PRIORITY? INTO _table (_insert_columns) VALUES (_insert_values)
    | REPLACE LOW_PRIORITY? INTO _table (_insert_columns) VALUES (_insert_values) returning_clause?

# FLUSH
flush:
    FLUSH flush_op

flush_op:
    NO_WRITE_TO_BINLOG flush_option
    | LOCAL flush_option
    | NO_WRITE_TO_BINLOG flush_table_option
    | LOCAL flush_table_option

flush_option:
    INDEX_STATISTICS
    | ERROR LOGS
    | ENGINE LOGS
    | GENERAL LOGS
    | SLOW LOGS
    | BINARY LOGS
    | RELAY LOGS
    | TABLE_STATISTICS
    | USER_STATISTICS
    | USER_VARIABLES

flush_option_more:
    , flush_option

flush_table_option:
    TABLES
    | TABLES _table_without_view

# RESET
reset:
    RESET SLAVE ALL?
    | RESET QUERY CACHE

# CREATE INDEX
create_index:
    CREATE or_replace? UNIQUE? INDEX _new_index_name index_type? ON _table(index_table_column) algorithm_option?
    | CREATE or_replace? FULLTEXT? INDEX _new_index_name ON _table(index_table_column) algorithm_option?

index_type:
    USING BTREE
    | USING HASH

index_table_column:
    indexed_column
    | indexed_column
    | indexed_column indexed_column_more*

indexed_column:
    _distinct_column primary_key_order?

primary_key_order:
    ASC
    | DESC

indexed_column_more:
    , indexed_column

# DROP INDEX
drop_index:
    DROP INDEX if_exists? _drop_index

lock_option:
    LOCK=DEFAULT
    | LOCK=NONE
    | LOCK=SHARED
    | LOCK=EXCLUSIVE

# TRUNCATE TABLE
truncate:
    TRUNCATE TABLE _table_without_view

exprs:
    expr
    @disable-oracle transaction_verifier
    | simple_expr
    | (expr) expr_more*
    @disable-oracle transaction_verifier
    | (simple_expr) expr_more*

expr_more:
    , (expr)

expr:
    (not_operator expr)                       # notExpression
    | (expr logical_operator expr)             # logicalExpression
    | (predicate IS NOT? test_value)          # isExpression
    | predicate

simple_expr:
    (not_operator expression_atom_constant)                    # notExpression
    | (expression_atom_constant logical_operator expression_atom_constant)    # logicalExpression
    | (expression_atom_constant IS NOT? test_value)            # isExpression
    | ((expression_atom_constant) NOT? IN ( exprs ))
    | ((expression_atom_constant) IS NOT? NULL)
    | ((expression_atom_constant) comparison_operator (expression_atom_constant))
    | ((expression_atom_constant) NOT? BETWEEN (expression_atom_constant) AND (expression_atom_constant))
    | ((expression_atom_constant) SOUNDS LIKE (expression_atom_constant))
    | ((expression_atom_constant) NOT? LIKE (expression_atom_constant))
    | ((expression_atom_constant) NOT? regex (expression_atom_constant))
    | expression_atom_constant
    | (unary_operator expression_atom_constant)                          # unaryExpressionAtom
    | BINARY expression_atom_constant                                    # binaryExpressionAtom
    | (temporal_value add_subtract_operator INTERVAL temporal )          # intervalExpressionAtom
    | (expression_atom_constant bit_operator expression_atom_constant)              # bitExpressionAtom
    | (expression_atom_constant mult_operator expression_atom_constant)             # mathExpressionAtom
    | (expression_atom_constant add_operator expression_atom_constant)              # mathExpressionAtom

not_operator:
    NOT
    | ! # '!' is deprecated and will be removed in a future release. Please use NOT instead

logical_operator:
    AND
    | {print("&&")}
    | XOR
    | OR
    | {print("||")}

test_value:
    TRUE
    | FALSE
    | UNKNOWN

predicate:
    (predicate NOT? IN ( _context simple_one_select _end ))
    @disable-oracle transaction_verifier
    | (predicate NOT? IN ( exprs ))
    | (predicate IS NOT? NULL)
    | (predicate comparison_operator predicate)
    | (predicate comparison_operator quantifier ( _context simple_one_select _end ))
    @disable-oracle transaction_verifier
    | (predicate NOT? BETWEEN predicate AND predicate)
    | (predicate SOUNDS LIKE predicate)
    | (predicate NOT? LIKE predicate)
    | (predicate NOT? regex predicate)
    | (expression_atom)

comparison_operator:
    {print("=")}
    | {print(">=")}
    | {print(">")}
    | {print("<=")}
    | {print("<")}
    | {print("!=")} # <>

quantifier:
    ALL
    | ANY
    | SOME

regex:
    REGEXP
    | RLIKE

expression_atom:
    constant                                                    # constantExpressionAtom
    | _column                                                   # fullColumnNameExpressionAtom
    | function_call                                             # functionCallExpressionAtom
    | (unary_operator expression_atom)                          # unaryExpressionAtom
    | BINARY expression_atom                                    # binaryExpressionAtom
    | EXISTS ( _context simple_one_select _end )                                         # existsExpressionAtom
    @disable-oracle transaction_verifier
    | ( _context simple_one_select _end )                                                # subqueryExpressionAtom
    @disable-oracle transaction_verifier
    | (temporal_value add_subtract_operator INTERVAL temporal )                                # intervalExpressionAtom
    | (expression_atom bit_operator expression_atom)              # bitExpressionAtom
    | (expression_atom mult_operator expression_atom)             # mathExpressionAtom
    | (expression_atom add_operator expression_atom)              # mathExpressionAtom

expression_atom_constant:
    constant                                                    # constantExpressionAtom
    | _column                                                   # fullColumnNameExpressionAtom
    | function_call

temporal_value:
    _microsecond
    | _second
    | _minute
    | _hour
    | _day
    | _int8
    | _month
    | _year
    | _second_microsecond
    | _minute_microsecond
    | _minute_second
    | _hour_microsecond
    | _hour_second
    | _hour_minute
    | _day_microsecond
    | _day_second
    | _day_minute
    | _day_hour
    | _year_month

temporal:
    _microsecond MICROSECOND
    | _second SECOND
    | _minute MINUTE
    | _hour HOUR
    | _day DAY
    | _int8 WEEK
    | _month MONTH
    | _int8 QUARTER
    | _year YEAR
    | _second_microsecond SECOND_MICROSECOND
    | _minute_microsecond MINUTE_MICROSECOND
    | _minute_second MINUTE_SECOND
    | _hour_microsecond HOUR_MICROSECOND
    | _hour_second HOUR_SECOND
    | _hour_minute HOUR_MINUTE
    | _day_microsecond DAY_MICROSECOND
    | _day_second DAY_SECOND
    | _day_minute DAY_MINUTE
    | _day_hour DAY_HOUR
    | _year_month YEAR_MONTH

add_subtract_operator:
    {print("+")}
    | {print("-")}

unary_operator:
    {print("!")}
    | {print("~")}
    | {print("+")}
    | {print("-")}
    | {print("NOT")}

bit_operator:
    {print("<<")}
    | {print(">>")}
    | {print("&")}
    | {print("^")}
    | {print("|")}

mult_operator:
    {print("*")}
    | {print("/")}
    | {print("%")}
    | {print("DIV")}
    | {print("MOD")}

add_operator:
    {print("+")}
    | {print("-")}

constant:
    NULL
    | TRUE
    | FALSE
    | _char
    | _text
    | integerl
    | CURRENT_TIME              @disable-symbol generated_constraint, create_index
                                @disable-oracle equation, write_guard
    | CURRENT_DATE              @disable-symbol generated_constraint, create_index
                                @disable-oracle equation, write_guard
    | CURRENT_TIMESTAMP         @disable-symbol generated_constraint, create_index
                                @disable-oracle equation, write_guard

integerl:
    _int8
    | _float
    | _double

function_call:
    math_func
    | cast_oper
    | control_flow_func
    | str_func
    | date_func
    | encrypt_func
    | information_func    @disable-oracle equation
    | misc_func
    | aggregate_func
    @disable-symbol expr_without_aggregate, aggregate_func                     # Nested aggregate functions are not allowed
    @disable-oracle transaction_verifier

aggregate_func:
   COUNT( _column )
   | AVG( _column )
   | SUM( _column )
   | MAX( _column )
   | MIN( _column )
   | GROUP_CONCAT( _column, _column )
   | BIT_AND( arg )
   | BIT_COUNT( arg )
   | BIT_LENGTH( arg )
   | BIT_OR( arg )
   | BIT_XOR( arg )

misc_func:
    DEFAULT( _column )    @disable-oracle equation
    | INET_ATON( arg )    @disable-oracle equation
    | INET_NTOA( arg )    @disable-oracle equation
    | NAME_CONST( const_char_value, value )
    | RAND()         @disable-oracle equation
    | RAND( arg )    @disable-oracle equation
    | SLEEP( zero_or_almost )    @disable-oracle equation
    | UUID()         @disable-oracle equation

zero_or_almost:
    0
    | 0.01

information_func:
    CONNECTION_ID()
    | CURRENT_USER()
    | CURRENT_USER
    | DATABASE()
    | SCHEMA()
    | FOUND_ROWS()
    | LAST_INSERT_ID()
    | ROW_COUNT()               @disable-oracle transaction_verifier

control_flow_func:
    (CASE arg WHEN arg THEN arg END)
    | (CASE arg WHEN arg THEN arg WHEN arg THEN arg END)
    | (CASE arg WHEN arg THEN arg ELSE arg END)
    | IF( arg, arg, arg )
    | IFNULL( arg, arg )
    | NULLIF( arg, arg )

cast_oper:
   BINARY arg
   | CAST( arg AS type )
   | CONVERT( arg, type )
   | CONVERT( arg USING charset )

type:
    BINARY
    | BINARY(_digit)
    | CHAR
    | CHAR(_digit)
    | DATE
    | DATETIME
    | DECIMAL
    | DECIMAL(decimal_m)
    | DECIMAL(decimal_m,decimal_d)
    | SIGNED
    | TIME
    | UNSIGNED

charset:
   utf8
   | latin1

encrypt_func:
    AES_DECRYPT( arg, arg )
    | AES_ENCRYPT( arg, arg )
    | COMPRESS( arg )
    | MD5( arg )
    | SHA1( arg )
    | SHA( arg )
    | SHA2( arg, arg )
    | UNCOMPRESS( arg )             @disable-oracle write_guard
    | UNCOMPRESSED_LENGTH( arg )    @disable-oracle write_guard

str_func:
    ASCII( arg )
    | BIN( arg )
    | BIT_LENGTH( arg )
    | CHAR_LENGTH( arg )
    | CHARACTER_LENGTH( arg )
    | CHAR( arg )
    | CHAR( arg USING charset )
    | CONCAT_WS( arg_list )
    | CONCAT( arg )
    | CONCAT( arg_list )
    | ELT( arg_list )
    | EXPORT_SET( arg, arg, arg )
    | EXPORT_SET( arg, arg, arg, arg )
    | EXPORT_SET( arg, arg, arg, arg, arg )
    | FIELD( arg_list )
    | FIND_IN_SET( arg, arg )
    | FORMAT( arg, arg )
    | FORMAT( arg, arg, locale )
    | HEX( arg )
    | INSERT( arg, arg, arg, arg )
    | INSTR( arg, arg )
    | LCASE( arg )
    | LEFT( arg, arg )
    | LENGTH( arg )
    | arg NOT? LIKE arg
    | LOCATE( arg, arg )
    | LOCATE( arg, arg, arg )
    | LOWER( arg )
    | LPAD( arg, arg, arg )
    | LTRIM( arg )
    | MAKE_SET( arg_list )
    | MID( arg, arg, arg )
    | OCT( arg )
    | OCTET_LENGTH( arg )
    | ORD( arg )
    | POSITION( arg IN arg )
    | QUOTE( arg )

date_func:
    ADDDATE( arg, INTERVAL arg unit1 )
    | ADDDATE( arg, arg )
    | ADDTIME( arg, arg )
    | CONVERT_TZ( arg, arg, arg )
    | CURDATE()
    | CURRENT_DATE()    @disable-oracle equation
    | CURRENT_DATE      @disable-oracle equation
    | CURTIME()         @disable-oracle equation
    | CURRENT_TIME()       @disable-oracle equation
    | CURRENT_TIMESTAMP()  @disable-oracle equation
    | DATE( arg )
    | DATEDIFF( arg, arg )
    | DATE_ADD( arg, INTERVAL arg unit1 )
    | DATE_SUB( arg, INTERVAL arg unit1 )
    | DATE_FORMAT( arg, arg )
    | DAY( arg )
    | DAYOFMONTH( arg )
    | DAYNAME( arg )
    | DAYOFWEEK( arg )
    | DAYOFYEAR( arg )
    | EXTRACT( unit1 FROM arg )
    | FROM_DAYS( arg )
    | FROM_UNIXTIME( arg )
    | FROM_UNIXTIME( arg, arg )
    | GET_FORMAT( get_format_type, get_format_format )
    | HOUR( arg )
    | LAST_DAY( arg )
    | LOCALTIME()
    | LOCALTIMESTAMP()    @disable-oracle equation
    | MAKEDATE( arg, arg )
    | MAKETIME( arg, arg, arg )
    | MICROSECOND( arg )
    | MINUTE( arg )
    | MONTH( arg )
    | MONTHNAME( arg )
    | NOW()    @disable-oracle equation
    | PERIOD_ADD( arg, arg )
    | PERIOD_DIFF( arg, arg )
    | QUARTER( arg )
    | SECOND( arg )
    | SEC_TO_TIME( arg )
    | STR_TO_DATE( arg, arg )
    | SUBDATE( arg, arg )
    | SUBTIME( arg, arg )
    | SYSDATE()    @disable-oracle equation
    | TIME( arg )
    | TIMEDIFF( arg, arg )
    | TIMESTAMP( arg )
    | TIMESTAMP( arg, arg )
    | TIMESTAMPADD( unit2, arg, arg )
    | TIMESTAMPDIFF( unit2, arg, arg )
    | TIME_FORMAT( arg, arg )
    | TIME_TO_SEC( arg )
    | TO_DAYS( arg )
    | TO_SECONDS( arg )
    | UNIX_TIMESTAMP( arg )
    | UNIX_TIMESTAMP()    @disable-oracle equation
    | UTC_DATE()          @disable-oracle equation
    | UTC_TIME()          @disable-oracle equation
    | UTC_TIMESTAMP()     @disable-oracle equation
    | WEEK( arg )
    | WEEK( arg, week_mode )
    | WEEKDAY( arg )
    | WEEKOFYEAR( arg )
    | YEAR( arg )
    | YEARWEEK( arg )
    | YEARWEEK( arg, week_mode )

week_mode:
    0
    | 1
    | 2
    | 3
    | 4
    | 5
    | 6
    | 7
    | arg

get_format_type:
    DATE
    | TIME
    | DATETIME

get_format_format:
    'EUR'
    | 'USA'
    | 'JIS'
    | 'ISO'
    | 'INTERNAL'
    | arg

unit1:
    MICROSECOND
    | SECOND
    | MINUTE
    | HOUR
    | DAY
    | WEEK
    | MONTH
    | QUARTER
    | YEAR
    | SECOND_MICROSECOND
    | MINUTE_MICROSECOND
    | MINUTE_SECOND
    | HOUR_MICROSECOND
    | HOUR_SECOND
    | HOUR_MINUTE
    | DAY_MICROSECOND
    | DAY_SECOND
    | DAY_MINUTE
    | DAY_HOUR
    | YEAR_MONTH

uint2:
    MICROSECOND
    | SECOND
    | MINUTE
    | HOUR
    | DAY
    | WEEK
    | MONTH

math_func:
    ABS( arg )
    | ACOS( arg )
    | ASIN( arg )
    | ATAN( arg )
    | ATAN( arg, arg )
    | ATAN2( arg, arg )
    | CEIL( arg )
    | CEILING( arg )
    | CONV( arg, _int4_unsigned, _int4_unsigned )
    | COS( arg )
    | COT( arg )
    | CRC32( arg )
    | DEGREES( arg )
    | EXP( arg )
    | FLOOR( arg )
    | FORMAT( arg, _digit )
    | FORMAT( arg, format_second_arg, locale )
    | HEX( arg )
    | LN( arg )
    | LOG( arg )
    | LOG( arg, arg )
    | LOG2( arg )
    | LOG10( arg )
    | MOD( arg, arg )
    | PI( )
    | POW( arg, arg )
    | POWER( arg, arg )
    | RADIANS( arg )
    | RAND()         @disable-oracle equation
    | RAND( arg )    @disable-oracle equation
    | ROUND( arg )
    | ROUND( arg, arg )
    | SIGN( arg )
    | SIN( arg )
    | SQRT( arg )
    | TAN( arg )
    | TRUNCATE( arg, truncate_second_arg )

arg_list:
    arg_list_2
    | arg_list_3
    | arg_list_5
    | arg_list_10
    | arg, arg_list

arg_list_2:
    arg, arg

arg_list_3:
    arg, arg, arg

arg_list_5:
    arg, arg, arg, arg, arg

arg_list_10:
    arg, arg, arg, arg, arg, arg, arg, arg, arg, arg

format_second_arg:
   truncate_second_arg

truncate_second_arg:
    _digit
    | _digit
    | _int4_unsigned
    | arg

arg:
    _column
    | value
    | ( function_call )
    @disable-oracle transaction_verifier

arg_char:
    _column
    | _text
    | _char
    | NULL

const_char_value:
    _char
    | _text
    | ''

value:
    _int8
    | _int16
    | _int4_unsigned
    | _int8_unsigned
    | _text
    | _digit
    | _datetime
    | _date
    | _time
    | NULL

locale:
    'en_US'
    | 'de_DE'

algorithm_option:
    ALGORITHM =? DEFAULT
   | ALGORITHM =? COPY

# ANALYZE
analyze_table:
    ANALYZE analyze_table_op? TABLE _table

analyze_table_op:
    NO_WRITE_TO_BINLOG
    | LOCAL

more_column:
    , _column

with_bucket:
    WITH _int16_unsigned BUCKETS

# CHECKSUM
checksum:
    CHECKSUM TABLE _table
    | CHECKSUM TABLE _table QUICK
    | CHECKSUM TABLE _table EXTENDED

# CHECK TABLE
check_table:
    CHECK TABLE _table check_option

check_option:
    FOR UPGRADE
    | QUICK
    | FAST
    | MEDIUM
    | EXTENDED
    | CHANGED

check_option_more:
    , check_option

# OPTIMIZE
optimize:
    OPTIMIZE analyze_table_op? TABLE _table wait_option?

wait_option:
    WAIT _int8_unsigned
    | NOWAIT

# REPAIR
repair:
    REPAIR analyze_table_op? TABLE _table QUICK? EXTENDED? USE_FRM?
    | REPAIR analyze_table_op? VIEW _view from_option?

from_option: FROM MYSQL

# BEGIN TRANSACTION
begin:
    BEGIN
    | BEGIN WORK @disable-oracle write_guard, transaction_verifier
    | START TRANSACTION
    | START TRANSACTION transaction_characteristic @disable-oracle write_guard, transaction_verifier
    # | SET AUTOCOMMIT = 0

transaction_characteristic:
    WITH CONSISTENT SNAPSHOT
    | READ WRITE
    | READ ONLY

# COMMIT
commit:
    COMMIT
    | COMMIT WORK? no_chain? no_release? @disable-oracle write_guard, transaction_verifier

no_chain:
    AND NO CHAIN

no_release:
    NO RELEASE

# ROLLBACK
rollback:
    ROLLBACK
    | ROLLBACK WORK? no_chain? no_release? @disable-oracle write_guard, transaction_verifier

# ISOLATION LEVEL
set_isolation:
    SET SESSION TRANSACTION ISOLATION LEVEL isolation_level

isolation_level:
    READ UNCOMMITTED
    | READ COMMITTED
    | REPEATABLE READ
    | SERIALIZABLE

# SET, not complete
set_variable:
    SET global_or_session autocommit = 1
    | SET global_or_session big_tables = on_or_off
    | SET GLOBAL completion_type = tree_value_option
    | SET global_or_session bulk_insert_buffer_size = _int16_unsigned
    | SET GLOBAL concurrent_insert = tree_value_option
    | SET GLOBAL DELAY_KEY_WRITE = on_or_off
    | SET GLOBAL eq_range_index_dive_limit = _int16_unsigned
    | SET GLOBAL FLUSH = on_or_off
    | SET global_or_session foreign_key_checks = two_value_option
    | SET GLOBAL host_cache_size = _int8_unsigned

global_or_session:
    GLOBAL
    | SESSION

on_or_off:
    OFF
    | ON

tree_value_option:
    0
    | 1
    | 2

on_off_all:
    ON
    | OFF
    | ALL

# not support SHOW TABLES [FROM db_name] [LIKE 'pattern' | WHERE expr]
show_tables:
    SHOW FULL? TABLES