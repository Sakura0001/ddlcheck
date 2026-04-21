/*
The following statements or features have not been supported yet.

---- CREATE TABLE t1 (c1 CHECK (**))
---- CREATE TABLE t1 (c1 INT PRIMARY KEY, FOREIGN KEY(c1) REFERENCES t2(c2))
---- CREATE TABLE t1 (c1 INT GENERATED ALWAYS AS (c2+1))

---- CREATE INDEX i1 ON t1(c1+c2); ** Not support indexes on expressions involving columns

---- CREATE VIEW v1 (c1, c2) AS SELECT c1 AS ca1 FROM t1;
---- CREATE VIEW v2 (c1, c2) AS SELECT c1 AS ca1 FROM v1; ** View v1 is not a temp view.

---- WHERE SOUNDEX ((c1)); ** It is only available if the SQLITE_SOUNDEX compile-time option is used when SQLite is built.

---- json_array_length(json_array, path) ** json file's path
---- json_extract(json, path, ...)
---- json_insert(json, path, value[, path2, value2] ...)
---- json_remove(json, path[, path] ...)

*/

# CREATE TABLE
create_table:
    CREATE temporary? TABLE if_not_exists? _new_table_name (first_new_column new_column_more* table_constraint?) table_options?
    
temporary:
    TEMP
    | TEMPORARY

if_not_exists:
    IF NOT EXISTS

first_new_column:
    new_column

new_column_more:
    , new_column

new_column:
    _new_column_name type_name column_constraint?

type_name:
    INT
    | TEXT
    | BLOB
    | REAL
    | INTEGER

column_constraint:
    PRIMARY KEY primary_key_order? conflict_clause? AUTOINCREMENT?
    @disable-symbol alter_table
    @disable-query {PRIMARY KEY.*PRIMARY KEY}                   # Cannot add two primary keys for a table
    @disable-query {^((?!INTEGER PRIMARY KEY).)*AUTOINCREMENT}  # AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY
    @disable-query {DESC[^,]*AUTOINCREMENT}                     # AUTOINCREMENT cannot appear with DESC
    | NOT NULL conflict_clause?
    | UNIQUE conflict_clause?        
    @disable-symbol alter_table                                 # Cannot add a UNIQUE column for alter table
    | check_expr
    | DEFAULT default_value
    | collation
    | generated_constraint
    @disable-symbol first_new_column

conflict_clause:
    ON CONFLICT ROLLBACK
    | ON CONFLICT ABORT
    | ON CONFLICT FAIL
    | ON CONFLICT IGNORE
    | ON CONFLICT REPLACE    

check_expr:
    CHECK(expr_without_aggregate)

default_value:
    literal_value
    | _int32

generated_constraint:
    AS (expr_without_aggregate) generated_option?
    | GENERATED ALWAYS AS (expr_without_aggregate) generated_option?

generated_option:
    STORED
    | VIRTUAL

primary_key_order:
    ASC
    | DESC

table_constraint:
    primary_key_table_constraint
    @disable-query {PRIMARY KEY.*PRIMARY KEY}               # Cannot add two primary keys for a table
    | , UNIQUE (_column more_column? more_column?)
    | , FOREIGN KEY (_column) foreign_key_clause
    | check_expr

primary_key_table_constraint:
    , PRIMARY KEY (_column more_column? more_column?)

# https://www.sqlite.org/syntax/foreign-key-clause.html
foreign_key_clause:
    REFERENCES _reference_table on_action? deferrable?

on_action:
    ON on_delete_or_update on_action_list

on_delete_or_update:
    DELETE
    | UPDATE

on_action_list:
    SET NULL
    | SET DEFAULT
    | CASCADE
    | RESTRICT
    | NO ACTION

deferrable:
    NOT? DEFERRABLE
    | NOT? DEFERRABLE INITIALLY DEFERRED
    | NOT? DEFERRABLE INITIALLY IMMEDIATE

more_column:
    , _column
    
table_options:
    WITHOUT ROWID
    @disable-query {^((?!PRIMARY KEY).)*WITHOUT ROWID}      # Must have PRIMARY KEY on WITHOUT ROWID tables
    @disable-query {AUTOINCREMENT.*WITHOUT ROWID}           # AUTOINCREMENT is not allowed on WITHOUT ROWID tables
    | STRICT

# CREATE VIRTUAL TABLE USING FTS4
create_virtual_table_using_fts4:
    CREATE VIRTUAL TABLE if_not_exists? _new_virtual_table USING fts4 (virtual_column virtual_column_more* fts4_module_option*)

virtual_column:
    _new_column_name UNINDEXED?

virtual_column_more:
    , virtual_column

fts4_module_option:
    , fts4_module_argument

fts4_module_argument:
    compress=zip, uncompress=unzip
    | matchinfo=fts3
    | tokenize=simple
    | tokenize=porter
    | tokenize=unicode61
    | prefix={temp = int4_unsigned(); print(temp)}
    | order=ASC
    | order=DESC
    | languageid="lid"
    | notindexed={temp = column(); print(temp)}

# CREATE VIRTUAL TABLE USING FTS5
create_virtual_table_using_fts5:
    CREATE VIRTUAL TABLE if_not_exists? _new_virtual_table USING fts5 (virtual_column virtual_column_more* fts5_module_option*)

fts5_module_option:
    , fts5_module_argument

fts5_module_argument:
    columnsize = columnsize_option
    | detail = detail_option
    | prefix = _int8_unsigned
    | tokenize = fts5_tokenize_option
    | content = ''

columnsize_option:
    0
    | 1

detail_option:
    full
    | column
    | none

fts5_tokenize_option:
    'porter ascii'
    | 'porter'
    | {print('"')}unicode61 unicode61_options*{print('"')}
    | 'ascii'

unicode61_options:
    remove_diacritics remove_diacritics_option
    | tokenchars {inputString = text(); print("'"); print(string.sub(inputString, 2, -2)); print("'")}
    | separators {inputString = text(); print("'"); print(string.sub(inputString, 2, -2)); print("'")}

remove_diacritics_option:
    0
    | 1
    | 2

# CREATE VIRTUAL TABLE USING RTree
# RTree requires an odd number of columns
create_virtual_table_using_rtree:
    CREATE VIRTUAL TABLE if_not_exists? _new_virtual_table USING rtree (_new_column_name, _new_column_name, _new_column_name)
    | CREATE VIRTUAL TABLE if_not_exists? _new_virtual_table USING rtree (_new_column_name, _new_column_name, _new_column_name, _new_column_name, _new_column_name)
    | CREATE VIRTUAL TABLE if_not_exists? _new_virtual_table USING rtree_i32 (_new_column_name, _new_column_name, _new_column_name)
    | CREATE VIRTUAL TABLE if_not_exists? _new_virtual_table USING rtree_i32 (_new_column_name, _new_column_name, _new_column_name, _new_column_name, _new_column_name)

# ALTER TABLE
alter_table:
    alter_table_rename_table
    | alter_table_rename_column
    | alter_table_add_column
    | alter_table_drop_column

alter_table_rename_table:
    ALTER TABLE _table RENAME TO _new_table_name

alter_table_rename_column:
    ALTER TABLE _table RENAME COLUMN? _column TO _new_column_name

alter_table_add_column:
    ALTER TABLE _table ADD COLUMN? new_column

alter_table_drop_column:
    ALTER TABLE _table DROP COLUMN? _drop_column

# DROP TABLE
drop_table:
    DROP TABLE if_exists? _table

if_exists:
    IF EXISTS

# CREATE INDEX
create_index:
    CREATE UNIQUE? INDEX if_not_exists? _new_index_name ON _table (indexed_column indexed_column_more*) where_clause?

indexed_column:
    column_or_expr collation? primary_key_order?

column_or_expr:
    _column

collation:
    COLLATE BINARY
    | COLLATE NOCASE
    | COLLATE RTRIM

indexed_column_more:
    , indexed_column

# DROP INDEX
drop_index:
    DROP INDEX if_exists? _index

# INSERT
# https://www.sqlite.org/lang_insert.html
insert:
    INSERT or_clause? INTO _table (_insert_columns) VALUES (_insert_values) upsert_clause?

or_clause:
    OR ABORT
    | OR FAIL
    | OR IGNORE
    | OR REPLACE
    | OR ROLLBACK

upsert_clause:
    ON CONFLICT DO NOTHING
    | ON CONFLICT DO update_set

update_set:
    UPDATE SET assignment assignment_more* where_clause?

# REPLACE
# https://www.sqlite.org/lang_replace.html
replace:
    REPLACE INTO _table (_insert_columns) VALUES (_insert_values) upsert_clause?

# UPDATE
# https://www.sqlite.org/lang_update.html
update:
    UPDATE or_clause? _table SET assignment assignment_more* where_clause?

assignment:
    _distinct_column = _value

assignment_more:
   , assignment

where_clause:
    WHERE expr_without_aggregate

# SELECT
# https://www.sqlite.org/lang_select.html
select:
    single_select order_by_option? limit_option?
    @disable-oracle norec, tlp_where, tlp_aggregate, tlp_group_by, tlp_having, transaction_verifier
    | compound_select order_by_option? limit_option?
    @disable-oracle norec, tlp_where, tlp_aggregate, tlp_group_by, tlp_having, transaction_verifier
    | SELECT _column FROM table_sources where_clause
    @enable-oracle norec, tlp_where
    | SELECT aggregate_expr AS _column_alias FROM table_sources where_clause order_by_option?
    @enable-oracle tlp_aggregate
    | SELECT select_field_list FROM table_sources where_clause group_by_option
    @enable-oracle tlp_group_by, tlp_having
    | SELECT distinct_or_all? select_field_list FROM table_sources group_by_option? order_by_option? limit_option?
    @enable-oracle transaction_verifier

single_select:
    select_without_aggregate
    | select_with_aggregate
    
select_without_aggregate:
    SELECT distinct_or_all? select_field_list FROM table_sources where_clause? group_by_option?

select_with_aggregate:
    SELECT distinct_or_all? select_field_list FROM table_sources where_clause? group_by_option

distinct_or_all:
    DISTINCT
    | ALL

select_field_list:
    select_field select_field_more*

select_field_more:
    , select_field

select_field:
    _column AS _column_alias
    | (expr_without_aggregate) AS _column_alias
    @disable-symbol select_with_aggregate, select3_with_aggregate
    | (expr_with_aggregate) AS _column_alias
    @disable-symbol select_without_aggregate, select3_without_aggregate
    @disable-oracle norec, tlp_where, tlp_group_by, tlp_having, transaction_verifier

single_select3:
    select3_without_aggregate
    | select3_with_aggregate
    
select3_without_aggregate:
    SELECT distinct_or_all? select3_field_list FROM table_sources where_clause?

select3_with_aggregate:
    SELECT distinct_or_all? select3_field_list FROM table_sources where_clause?

select3_field_list:
    select_field, select_field, select_field

compound_select:
    _context single_select3 _return compound_select_more+
    
compound_select_more:
    compound_op _context single_select3 _end

compound_op:
    UNION
    | UNION ALL
    | INTERSECT
    | EXCEPT

table_sources:
    table_source
    | table_source table_source_more*
    @disable-oracle transaction_verifier

table_source_more:
    , table_source

table_source:
    _table
    | _table
    | _table
    | _table
    | _context (select) _return                 @disable-oracle tlp_having, norec, transaction_verifier
    | _context (join) _return                   @disable-oracle transaction_verifier

group_by_option:
    GROUP BY group_by_expr having_option?       @disable-oracle tlp_group_by, tlp_having
    | GROUP BY group_by_expr                    @enable-oracle tlp_group_by
    | GROUP BY group_by_expr having_option      @enable-oracle tlp_having

group_by_expr:
    _column
    | TRUE      @disable-oracle tlp_having
    | FALSE     @disable-oracle tlp_having

# HAVING expr can refer alias in select clause
# For example, SELECT c1 AS a1 FROM t1 GROUP BY c1 HAVING a1 > 10
having_option:
    HAVING expr_with_aggregate

order_by_option:
    ORDER BY ordering_term more_ordering_term?

ordering_term:
    _column collation? primary_key_order? nulls_option?

more_ordering_term:
    , ordering_term

nulls_option:
    NULLS FIRST
    | NULLS LAST

limit_option:
    LIMIT _int32_unsigned offset?

offset:
    OFFSET _int32_unsigned
    | , _int32_unsigned

join:
    table_source join_type table_source on_or_use_clause?
    | table_source NATURAL JOIN table_source

join_type:
    JOIN
    | CROSS JOIN
    | INNER JOIN
    | LEFT JOIN

on_or_use_clause:
    ON expr_without_aggregate
    | USING (_using_column)

# DELETE
# https://www.sqlite.org/lang_delete.html
delete:
    DELETE FROM _table where_clause?

# Modify Statistics Table
# here table should not have a alias
modify_sqlite_stat1:
    DELETE FROM _sqlite_stat1 where_clause?
    | INSERT OR IGNORE INTO _sqlite_stat1 VALUES ({print("'")}_table{print("'")}, {print("'")}_index{print("'")}, {print("'")}_int16_unsigned+ average_row_size? unordered? noskipscan?{print("'")})
    | INSERT OR IGNORE INTO _sqlite_stat1 VALUES ({print("'")}_table{print("'")}, {print("'")}_table{print("'")}, {print("'")}_int16_unsigned+ average_row_size? unordered? noskipscan?{print("'")})

average_row_size:
    sz = _int16_unsigned

# BEGIN TRANSACTION
begin:
    BEGIN TRANSACTION
    | BEGIN DEFERRED TRANSACTION
    | BEGIN IMMEDIATE TRANSACTION
    | BEGIN EXCLUSIVE TRANSACTION

# COMMIT
commit:
    COMMIT
    | END
    | COMMIT TRANSACTION
    | END TRANSACTION

# ROLLBACK
rollback:
    ROLLBACK TRANSACTION
    | ROLLBACK

# CREATE TRIGGER
create_trigger:
    CREATE temporary? TRIGGER if_not_exists? _new_trigger_name trigger_order? trigger_condition ON _table on_trigger_option? BEGIN trigger_action; more_trigger_action* END
    | CREATE temporary? TRIGGER if_not_exists? _new_trigger_name INSTEAD OF? trigger_condition ON _view on_trigger_option? BEGIN trigger_action; more_trigger_action* END

trigger_order:
    BEFORE
    | AFTER

trigger_condition:
    DELETE
    | INSERT
    | UPDATE trigger_update_column?

trigger_update_column:
    OF _column more_column*

on_trigger_option:
    FOR EACH ROW when_expr?
    | when_expr

when_expr:
    WHEN expr_with_aggregate

trigger_action:
    _context update _return
    | _context insert _return
    | _context delete _return
    | _context select _return

more_trigger_action:
    trigger_action;

# CREATE VIEW
create_view:
    CREATE temporary? VIEW if_not_exists? _new_view_name AS _context select_create_view _return

select_create_view:
    SELECT distinct_or_all? select_field_list FROM table_sources_without_temp where_clause?

sub_select_create_view:
    SELECT distinct_or_all? select_field FROM table_sources_without_temp where_clause?

table_sources_without_temp:
    table_source_without_temp
    | table_source_without_temp table_source_without_temp_more*

table_source_without_temp:
    _table_without_temp
    | _table_without_temp
    | _table_without_temp
    | _table_without_temp
    | _context (select_create_view) _return
    | _context (table_source_without_temp join_type table_source_without_temp) _return

table_source_without_temp_more:
    , table_source_without_temp

# DROP VIEW
drop_view:
    DROP VIEW if_exists? _view

# ANALYZE
analyze:
    ANALYZE
    | ANALYZE _index
    | ANALYZE _table
    | ANALYZE sqlite_master
    | ANALYZE main
    | ANALYZE temp

# REINDEX
reindex:
    REINDEX _index
    | REINDEX _table_without_temp
    | REINDEX BINARY
    | REINDEX NOCASE
    | REINDEX RTRIM

# VACUUM
vacuum:
    VACUUM
    | VACUUM temp
    | VACUUM main

# Check RTree table
check_rtree_table:
    SELECT rtreecheck({print("'")}_virtual_table{print("'")})

# https://www.sqlite.org/fts5.html
# rank is an auxiliary column in fts5
insert_fts5_command:
    INSERT INTO _virtual_table (_selected_virtual_table) VALUES (fts5_command)
    | INSERT INTO _virtual_table (_selected_virtual_table, rank) VALUES (fts5_command_with_rank)

fts5_command:
    'delete_all'
    | 'integrity-check'
    | 'optimize'
    | 'rebuild'

fts5_command_with_rank:
    'automerge', _int4_unsigned
    | 'crisismerge', _int8_unsigned
    | 'deletemerge', _int8_unsigned
    | 'integrity-check', 0
    | 'integrity-check', 1
    | 'merge', _int8_unsigned
    | 'pgsz', _int16_unsigned
    | 'rank', 'bm25(10.0, 5.0)'
    | 'secure-delete', 1
    | 'usermerge', _int4_unsigned        # 2-16

# https://www.sqlite.org/fts3.html
insert_fts4_command:
    INSERT INTO _virtual_table (_selected_virtual_table) VALUES (fts4_command)

fts4_command:
    'optimize'
    | 'rebuild'
    | 'integrity-check'
    | {print("'")}merge={x = int8_unsigned(); y = int4_unsigned(); print(x); print(","); print(y);}{print("'")}             # 'merge=X,Y' in which X and Y are integers. X can be any positive integer, Y should be between 2 and 16.
    | {print("'")}automerge={x = int4_unsigned(); print(x);}{print("'")}        # 'automerge=N', in which N is an integer between 0 and 15, inclusive.

expr_without_aggregate:
    (expr)
   
expr_with_aggregate:
    (expr)

expr:
    literal_value                                   @disable-symbol select
    | _column
    | binary_expr
    | unary_expr
    | (expr) NOT? BETWEEN (expr) AND (expr)
    | CAST (expr AS type_name)
    | func_expr
    | exists_or_not? (_context SELECT distinct_or_all? select_field FROM table_sources where_clause? _end)
    @disable-symbol check_expr, generated_constraint, create_index
    @disable-oracle tlp_having, transaction_verifier
    | (expr IN (expr exprs*))
    | (expr COLLATE collate_name)
    | (CASE expr? when_content+ else_content? END)
    | aggregate_expr
    @disable-symbol expr_without_aggregate, aggregate_expr                     # Nested aggregate functions are not allowed

binary_expr:
    (expr AND expr)
    | (expr OR expr)
    | (expr {print("||")} expr)
    | (expr {print("*")} expr)
    | (expr {print("/")} expr)
    | (expr % expr)
    | (expr {print("+")} expr)
    | (expr - expr)
    | (expr << expr)
    | (expr >> expr)
    | (expr & expr)
    | (expr {print("|")} expr)
    | (expr binary_comparison_op expr)
    # | (expr MATCH expr)        @disable-symbol generated_constraint, create_index

unary_expr:
    (- (expr))
    | ({print("+")} expr)
    | (~ expr)
    | (NOT expr)


expr_with_postfix:
    (expr ISNULL)
    | (expr NOTNULL)
    | (expr NOT NULL)
    | (expr IS TRUE)
    | (expr IS FALSE)

aggregate_expr:
    AVG(expr)                @disable-oracle tlp_aggregate
    | COUNT(expr)            @disable-oracle tlp_aggregate
    | GROUP_CONCAT(expr)     @disable-oracle tlp_aggregate
    | MAX(expr)
    | MIN(expr)
    | SUM(expr)
    | TOTAL(expr)

binary_comparison_op:
    <
    | <=
    | >
    | =
    | ==
    | !=
    | <>
    | IS NOT?
    | LIKE
    | GLOB

func_expr:
    ABS(expr)
    | CHANGES()                             @disable-symbol generated_constraint, create_index
    | CHAR(expr exprs*)
    | COALESCE(expr, expr exprs*)
    | GLOB(expr, expr)
    | HEX(expr)
    | LIKELY(expr)
    | IFNULL(expr, expr)
    | NULLIF(expr, expr)
    | LAST_INSERT_ROWID()                   @disable-symbol generated_constraint, create_index
    | LENGTH(expr)
    | LIKE(expr, expr)
    | LIKE(expr, expr, _char)
   # Other execution error: [SQLITE_ERROR] SQL error or missing database (not authorized)
   # | LOAD_EXTENSION(expr)                  @disable-symbol generated_constraint, create_index
   # | LOAD_EXTENSION(expr, expr)            @disable-symbol generated_constraint, create_index
    | LTRIM(expr, expr)
    | LTRIM(expr)
    | MAX(expr, expr+)
    | MIN(expr, expr+)
    | PRINTF(expr exprs*)
    | QUOTE(expr)
    | ROUND(expr, expr)
    | RTRIM(expr)
    | SQLITE_COMPILEOPTION_GET(expr)        @disable-symbol generated_constraint, create_index
    | SQLITE_COMPILEOPTION_USED(expr)       @disable-symbol generated_constraint, create_index
    | SQLITE_SOURCE_ID()                    @disable-symbol generated_constraint, create_index, create_view
    | SQLITE_VERSION()                      @disable-symbol generated_constraint, create_index, create_view
    | SUBSTR(expr, expr)
    | TOTAL_CHANGES()                       @disable-symbol generated_constraint, create_index, create_view
    | TRIM(expr)
    | UNICODE(expr)
    | UNLIKELY(expr)
    | UPPER(expr)
    | DATE(expr, expr exprs+)
    | TIME(expr, expr exprs+)
    | DATETIME(expr, expr exprs+)
    | JULIANDAY(expr, expr exprs+)
    | STRFTIME(expr, expr exprs+)
    | json_func
    | rtreenode(expr, expr)                 @disable-symbol create_index
    # | highlight(expr, expr, expr, expr)     @disable-symbol generated_constraint, create_index

json_func:
    json(expr)
    | json_array(expr exprs*)
    # | json_array_length(expr, expr)   # The second argument needs to be a path of json file
    | json_array_length(expr)
    # | json_extract(expr exprs+)
    # | json_insert(expr, expr exprs+)
    | json_object(expr exprs+)
    | json_patch(expr, expr)
    # | json_remove(expr exprs+)
    | json_valid(expr)
    | json_quote(expr)

exprs:
    , (expr)

exists_or_not:
    NOT? EXISTS

collate_name:
    NOCASE
    | RTRIM
    | BINARY

when_content:
    WHEN (expr) THEN (expr)

else_content:
    ELSE (expr)

literal_value:
    NULL
    | TRUE
    | FALSE
    | _char
    | _text
    | integerl
    | CURRENT_TIME              @disable-symbol generated_constraint, create_index
                                @disable-oracle equation
    | CURRENT_DATE              @disable-symbol generated_constraint, create_index
                                @disable-oracle equation
    | CURRENT_TIMESTAMP         @disable-symbol generated_constraint, create_index
                                @disable-oracle equation
integerl:
    _int8
    | _float
    | _double

#EXPLAIN
explain:
    EXPLAIN query
    | EXPLAIN QUERY PLAN query

query:
    alter_table
    | analyze
    | begin
    | commit
    | create_index
    | create_table
    | create_trigger
    | create_view
    | create_virtual_table_using_fts4
    | create_virtual_table_using_fts5
    | create_virtual_table_using_rtree
    | delete
    | drop_index
    | drop_table
    | drop_view
    | insert
    | pragma
    | reindex
    | rollback
    | select
    | update
    | vacuum
    
# PRAGMA
pragma:
    PRAGMA pragma_option

# Use the grammar in https://www.sqlite.org/pragma.html
pragma_option:
    analysis_limit = _int32
    | pragma_schema? application_id = _int32
    | pragma_schema? auto_vacuum = auto_vacuum_value
    | automatic_index = _bool
    | busy_timeout = _int16_unsigned                        # (0, 2^31-1 milliseconds)
    | pragma_schema? cache_size = cache_size_value          # (10, 2000*1000)
    | cache_spill = _bool
    | pragma_schema? cache_spill = _int32
    | case_sensitive_like = _bool
    | cell_size_check = _bool
    | checkpoint_fullfsync = _bool
    | collation_list
    | compile_options
    | pragma_schema? data_version
    | database_list
    | defer_foreign_keys = _bool
    | encoding = encoding_value
    | foreign_key_check
    | foreign_key_check(_table)
    | foreign_key_list(_table)
    | foreign_keys = _bool
    | pragma_schema? freelist_count
    | fullfsync = _bool
    | function_list
    | hard_heap_limit = _int16_unsigned
    | ignore_check_constraints = _bool
    | pragma_schema? incremental_vacuum
    | pragma_schema? incremental_vacuum (_int32_unsigned)
    | pragma_schema? index_info(_index)
    | pragma_schema? index_list(_table)
    | pragma_schema? index_xinfo(_index)
    | pragma_schema? integrity_check
    | pragma_schema? integrity_check (_table_without_temp)  # why limit not non-temp table?
    | pragma_schema? integrity_check (_int16_unsigned)
    | pragma_schema? journal_mode = journal_mode_value
    | pragma_schema? journal_size_limit = _int32
    | legacy_alter_table = _bool
    | legacy_file_format
    | pragma_schema? locking_mode = locking_mode_value
    | pragma_schema? max_page_count = _int32
    | pragma_schema? mmap_size = _int32_unsigned            # (0, 2147483646)
    | module_list
    | pragma_schema? optimize
    | pragma_schema? optimize(mask)                         # https://www.sqlite.org/pragma.html#pragma_optimize
    | pragma_schema? page_count
    | pragma_schema? page_size = page_size_value            # a power of two between 512 and 65536 inclusive.
    | parser_trace = _bool
    | pragma_list
    | query_only = _bool
    | pragma_schema? quick_check(_int32)
    | quick_check(_table)
    | main.quick_check(_table_without_temp)
    | temp.quick_check(_table_temp)
    | read_uncommitted = _bool
    | recursive_triggers = _bool
    | reverse_unordered_selects = _bool
    | pragma_schema? schema_version = _int32                # PRAGMA schema_version=N can lead to incorrect answers and/or database corruption
    | pragma_schema? secure_delete = _bool
    | pragma_schema? secure_delete = FAST
    | shrink_memory
    | soft_heap_limit = _int32_unsigned                     # sqlite3_int64 N (-9223372036854775808,+9223372036854775807)
    | stats
    | pragma_schema? synchronous = synchronous_value
    | pragma_schema? table_info(_table)
    | pragma_schema? table_list
    | pragma_schema? table_list(_table)
    | pragma_schema? table_xinfo(_table)
    | temp_store = temp_store_value
    | threads = _int16_unsigned                             # (0, 32767)
    | trusted_schema = _bool
    | pragma_schema? user_version = _int32                  # (1, 2147483647(2^31-1))
    | vdbe_addoptrace = _bool
    | vdbe_debug = _bool
    | vdbe_listing = _bool
    | vdbe_trace = _bool
    | pragma_schema? wal_autocheckpoint = _int32            # (0, 2^31-1)
    | pragma_schema? wal_checkpoint (wal_checkpoint_value)
    | writable_schema = _bool    # misuse of this pragma can easily result in a corrupt database file
    | writable_schema = RESET    # misuse of this pragma can easily result in a corrupt database file

pragma_schema:
    main.
    | temp.

auto_vacuum_value:
    0 | NONE | 1 | FULL | 2 | INCREMENTAL

cache_size_value:
    10 | 100 | 1000 | 100000 | 1000000

encoding_value:
    'UTF-8' | 'UTF-16' | 'UTF-16le' | 'UTF-16be'

journal_mode_value:
    DELETE | TRUNCATE | PERSIST | MEMORY | WAL | OFF

locking_mode_value:
    NORMAL | EXCLUSIVE

mask:
    0xfffe | -1 | 0x02                                        # More optimize mask can be set.

page_size_value:
    512 | 1024 | 2048 | 4096 | 8192 | 16384 | 32768 | 65536

synchronous_value:
    0 | OFF | 1 | NORMAL | 2 | FULL | 3 | EXTRA

temp_store_value:
    0 | DEFAULT | 1 | FILE | 2 | MEMORY

wal_checkpoint_value:
    PASSIVE | FULL | RESTART | TRUNCATE