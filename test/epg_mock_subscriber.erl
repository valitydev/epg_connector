-module(epg_mock_subscriber).

-export([
    handle_replication_data/2,
    handle_replication_stop/2
]).

handle_replication_data(_, _) ->
    ok.

handle_replication_stop(_, _) ->
    ok.
