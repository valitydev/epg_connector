-module(epg_mock_subscriber).

-export([
    handle_replication_data/1,
    handle_replication_stop/1
]).

handle_replication_data(_) ->
    ok.

handle_replication_stop(_) ->
    ok.
