-module(ejsondb).

-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-endif.

-include("ejsondb.hrl").

-export([
  new/2, 

  get/4, 
  set/5, 
  add/5, 
  add_at/6, 
  delete/4,

  get_all/4,
  set_all/5
]).

new(IdKey, QueryTab) ->
  #ejsondb_schema{qtab = QueryTab, id_key = IdKey}.
  
get(Qid, Ids, JsonStore, #ejsondb_schema{id_key = IdKey} = Schema) ->
  case qfind_i(Qid, Schema) of
    {error, Err} -> 
      {error, Err};
    {Query, Arity} when Arity + 1 =:= length(Ids) -> 
      PathIds = lists:droplast(Ids),
      Id = lists:last(Ids),
      FormattedQuery = qfmt_i(Query, PathIds),
      case ejsondb_crud:get_by(FormattedQuery, IdKey, Id, JsonStore) of
        {error, Err} -> {error, Err};
        {[], []} -> {error, not_found};
        {[Result], _Path} -> Result
      end;
    _ -> 
      {error, badarity}
  end.

set(Qid, Ids, Value, JsonStore, Schema = #ejsondb_schema{}) ->
  modify_i(put, Qid, Ids, Value, JsonStore, '$current', Schema).
add(Qid, Ids, Value, JsonStore, Schema = #ejsondb_schema{}) ->
  modify_i(create, Qid, Ids, Value, JsonStore, '$tail', Schema).
add_at(Qid, Ids, Value, JsonStore, At, Schema = #ejsondb_schema{}) ->
  modify_i(create, Qid, Ids, Value, JsonStore, At, Schema).

delete(Qid, Ids, JsonStore, #ejsondb_schema{id_key = IdKey} = Schema) ->
  case qfind_i(Qid, Schema) of
    {error, Err} -> 
      {error, Err};
    {Query, Arity} when Arity + 1 =:= length(Ids) -> 
      PathIds = lists:droplast(Ids),
      Id = lists:last(Ids),
      FormattedQuery = qfmt_i(Query, PathIds),
      case ejsondb_crud:delete(FormattedQuery, IdKey, Id, JsonStore) of
        {error, Err} -> {error, Err};
        NewJsonStore -> NewJsonStore
      end;
    _ -> 
      {error, badarity}
  end.
  
get_all(Qid, Ids, JsonStore, Schema = #ejsondb_schema{}) ->
  case qfind_i(Qid, Schema) of
    {error, Err} ->
      {error, Err};
    {Query, Arity} when Arity =:= length(Ids) -> 
      FormattedQuery = qfmt_i(Query, Ids),
      case ejsondb_crud:get(FormattedQuery, JsonStore) of
        {error, Err} -> {error, Err};
        {[], []} -> {error, not_found};
        {[Result], _Paths} -> Result
      end;
    _ -> {error, badarity}
  end.

set_all(Qid, Ids, Value, JsonStore, Schema = #ejsondb_schema{}) ->
  case qfind_i(Qid, Schema) of
    {error, Err} ->
      {error, Err};
    {Query, Arity} when Arity =:= length(Ids) ->
      FormattedQuery = qfmt_i(Query, Ids),
      SetAllFunc = 
      fun ({match, _}) -> {ok, Value};
          ({not_found, _, _, _}) -> erlang:error(not_found)
      end,
      case ejsondb_crud:set(FormattedQuery, JsonStore, SetAllFunc) of
        {error, Err} -> {error, Err};
        {NewJsonStore, _Paths} -> NewJsonStore
      end;
    _ -> {error, badarity}
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

modify_i(Op, Qid, Ids, Value, JsonStore, At, #ejsondb_schema{id_key = IdKey} = Schema) 
  when Op == put orelse Op == create ->
  case qfind_i(Qid, Schema) of
    {error, Err} ->
      {error, Err};
    {Query, Arity} when Arity + 1 =:= length(Ids) ->
      PathIds = lists:droplast(Ids),
      Id = lists:last(Ids),
      FormattedQuery = qfmt_i(Query, PathIds),
      case ejsondb_crud:Op(FormattedQuery, IdKey, Id, Value, JsonStore, At) of
        {error, Err} -> {error, Err};
        NewJsonStore -> NewJsonStore
      end;
    _ -> {error, badarity}
  end;
modify_i(_,_,_,_,_,_,_) ->
  erlang:error(bad_operation).

-spec qfmt_i(Query :: string(), Ids :: list()) ->
  string().

qfmt_i(Query, Ids) ->
  lists:flatten(io_lib:format(Query, Ids)).

-spec qfind_i(QueryId :: term(), #ejsondb_schema{}) -> {string(), number()} | {error, badkey}.

qfind_i(Qid, #ejsondb_schema{qtab = QTab}) ->
  case lists:keyfind(Qid, 1, QTab) of
    {Qid, Query, Arity} -> {Query, Arity};
    _ -> {error, badkey}
  end.