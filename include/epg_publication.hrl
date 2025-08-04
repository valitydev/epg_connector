-record(publication, {
    name :: string(),
    tables :: #{
       binary() => pgpool_wal_reader:table_description()
    }
}).
