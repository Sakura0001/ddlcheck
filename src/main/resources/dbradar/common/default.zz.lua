tables = {
    rows = {0, 1, 5, 10, 100},
    -- character set
    charsets = {'undef'},
    -- partition number
    partitions = {'undef'},
}

columns = {
    types = {'INT', 'VARCHAR', 'DATE', 'TIME', 'DATETIME'},
    keys = {'key', 'undef'},
    sign = {'signed'},
}

data = {
    INT = {'digit', 'digit', 'digit', 'digit', 'null'},
    VARCHAR = {'char', 'char', 'char', 'char', 'null'},
    DATE = {'date', 'time', 'datetime', 'year', 'timestamp', 'null'},
    TIME = {'date', 'time', 'datetime', 'year', 'timestamp', 'null'},
    DATETIME = {'date', 'time', 'datetime', 'year', 'timestamp', 'null'},
    ENUM = {'char', 'char', 'char', 'char', 'null'},
    BLOB = {'char', 'null'},
    BINARY = {'char', 'char', 'char', 'char', 'null'}
}