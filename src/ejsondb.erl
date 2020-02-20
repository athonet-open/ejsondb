-module(ejsondb).

-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-endif.

-include("ejsondb.hrl").

-export([
  new/2,
  new/4,

  get/4, 
  set/5, 
  add/5, 
  add_at/6, 
  delete/4,

  get_all/4,
  set_all/5
]).

%% @doc create ejsondb handle

-spec new(IdKey :: ejsondb_id_key(), QueryTab :: list(ejsondb_query_entry())) ->
  #ejsondb_schema{}.

new(IdKey, QueryTab)
  when is_atom(IdKey) orelse is_binary(IdKey) ->
  new(IdKey, QueryTab, #{}, []).


%% @doc create ejsondb handle with modifier funs and options.

-spec new(IdKey, QueryTab, Funs, Opts) ->
  #ejsondb_schema{}
when
  IdKey :: ejsondb_id_key(),
  QueryTab :: list(ejsondb_query_entry()),
  Funs :: ejsondb_crud:modifier_funs(),
  Opts :: ejsondb_crud:opts().

new(IdKey, QueryTab, Funs, Opts)
  when is_atom(IdKey) orelse is_binary(IdKey) ->
  #ejsondb_schema{qtab = QueryTab, id_key = IdKey, funs = Funs, opts = Opts}.


%% @doc get from json using query Qid, searching using Ids.
%% result is the value searched or not_found.

-spec get(Qid, Ids, JsonStore, Schema)
      -> {ok, Result} | {error, Reason}
  when
  Qid :: atom(),
  Ids :: list(ejsondb_crud:id_value()),
  JsonStore :: ejsondb_crud:json_node(),
  Schema :: #ejsondb_schema{},
  Result :: ejsondb_crud:json_node(),
  Reason :: not_found | query_not_found | badarity | term().

get(Qid, Ids, JsonStore, #ejsondb_schema{id_key = IdKey, funs = Funs, opts = Opts} = Schema) ->
  case qfind_i(Qid, Schema) of
    {error, Err} ->
      {error, Err};
    {Query, Arity} when Arity + 1 =:= length(Ids) ->
      PathIds = lists:droplast(Ids),
      Id = lists:last(Ids),
      FormattedQuery = qfmt_i(Query, PathIds),
      case ejsondb_crud:get_by(FormattedQuery, IdKey, Id, JsonStore, Funs, Opts) of
        {error, Err} -> {error, Err};
        {[], []} -> {error, not_found};
        {[Result], _Path} -> {ok, Result}
      end;
    _ -> 
      {error, badarity}
  end.


%% @doc set to json the value Value using query Qid, searching using Ids.
%% if not found create otherwise replace existing.
%% returns the modified json or error if invalid query or path not found.

-spec set(Qid, Ids, Value, JsonStore, Schema)
      -> {ok, Result} | {error, Reason}
  when
  Qid :: atom(),
  Ids :: list(ejsondb_crud:id_value()),
  Value :: ejsondb_crud:json_node(),
  JsonStore :: ejsondb_crud:json_node(),
  Schema :: #ejsondb_schema{},
  Result :: ejsondb_crud:json_node(),
  Reason :: not_found | query_not_found | badarity | term().

set(Qid, Ids, Value, JsonStore, Schema = #ejsondb_schema{}) ->
  modify_i(put, Qid, Ids, Value, JsonStore, '$current', Schema).


%% @doc append to json the value Value using query Qid, searching using Ids.
%% fail if already exists.
%% returns the modified json or error if invalid query or path not found.

-spec add(Qid, Ids, Value, JsonStore, Schema)
      -> {ok, Result} | {error, Reason}
  when
  Qid :: atom(),
  Ids :: list(ejsondb_crud:id_value()),
  Value :: ejsondb_crud:json_node(),
  JsonStore :: ejsondb_crud:json_node(),
  Schema :: #ejsondb_schema{},
  Result :: ejsondb_crud:json_node(),
  Reason :: not_found | query_not_found | badarity | term().

add(Qid, Ids, Value, JsonStore, Schema = #ejsondb_schema{}) ->
  modify_i(create, Qid, Ids, Value, JsonStore, '$tail', Schema).


%% @doc insert at position At in json the value Value using query Qid, searching using Ids.
%% fail if already exists.
%% returns the modified json or error if invalid query or path not found.

-spec add_at(Qid, Ids, Value, JsonStore, At, Schema)
      -> {ok, Result} | {error, Reason}
  when
  Qid :: atom(),
  Ids :: list(ejsondb_crud:id_value()),
  Value :: ejsondb_crud:json_node(),
  JsonStore :: ejsondb_crud:json_node(),
  At :: ejsondb_crud:insert_pos(),
  Schema :: #ejsondb_schema{},
  Result :: ejsondb_crud:json_node(),
  Reason :: not_found | query_not_found | badarity | term().

add_at(Qid, Ids, Value, JsonStore, At, Schema = #ejsondb_schema{}) ->
  modify_i(create, Qid, Ids, Value, JsonStore, At, Schema).


%% @doc delete using query Qid, searching using Ids.
%% if not found return success gracefully.
%% returns the modified json or error if invalid query.

-spec delete(Qid, Ids, JsonStore, Schema)
      -> {ok, Result} | {error, Reason}
  when
  Qid :: atom(),
  Ids :: list(ejsondb_crud:id_value()),
  JsonStore :: ejsondb_crud:json_node(),
  Schema :: #ejsondb_schema{},
  Result :: ejsondb_crud:json_node(),
  Reason :: query_not_found | badarity | term().

delete(Qid, Ids, JsonStore, #ejsondb_schema{id_key = IdKey, funs = Funs, opts = Opts} = Schema) ->
  case qfind_i(Qid, Schema) of
    {error, Err} -> 
      {error, Err};
    {Query, Arity} when Arity + 1 =:= length(Ids) -> 
      PathIds = lists:droplast(Ids),
      Id = lists:last(Ids),
      FormattedQuery = qfmt_i(Query, PathIds),
      case ejsondb_crud:delete(FormattedQuery, IdKey, Id, JsonStore, Funs, Opts) of
        {error, Err} -> {error, Err};
        NewJsonStore -> {ok, NewJsonStore}
      end;
    _ -> 
      {error, badarity}
  end.


%% @doc get_all for query Qid, searching using Ids.
%% returns an array of json_objects or error if invalid query or path not_found.

-spec get_all(Qid, Ids, JsonStore, Schema)
      -> {ok, Result} | {error, Reason}
  when
  Qid :: atom(),
  Ids :: list(ejsondb_crud:id_value()),
  JsonStore :: ejsondb_crud:json_node(),
  Schema :: #ejsondb_schema{},
  Result :: list(ejsondb_crud:json_node()),
  Reason :: not_found | query_not_found | badarity | term().

get_all(Qid, Ids, JsonStore, Schema = #ejsondb_schema{funs = Funs, opts = Opts}) ->
  case qfind_i(Qid, Schema) of
    {error, Err} ->
      {error, Err};
    {Query, Arity} when Arity =:= length(Ids) -> 
      FormattedQuery = qfmt_i(Query, Ids),
      case ejsondb_crud:get(FormattedQuery, JsonStore, Funs, Opts) of
        {error, Err} -> {error, Err};
        {[], []} -> {error, not_found};
        {[Result], _Paths} -> {ok, Result}
      end;
    _ -> {error, badarity}
  end.


%% @doc replace items found with query Qid with Values, searching using Ids.
%% returns the modified json or error if invalid query.

-spec set_all(Qid, Ids, Values, JsonStore, Schema)
      -> {ok, Result} | {error, Reason}
  when
  Qid :: atom(),
  Ids :: list(ejsondb_crud:id_value()),
  Values :: list(ejsondb_crud:json_node()),
  JsonStore :: ejsondb_crud:json_node(),
  Schema :: #ejsondb_schema{},
  Result :: ejsondb_crud:json_node(),
  Reason :: not_found | query_not_found | badarity | term().

set_all(Qid, Ids, Values, JsonStore, Schema = #ejsondb_schema{funs = Funs, opts = Opts}) ->
  case qfind_i(Qid, Schema) of
    {error, Err} ->
      {error, Err};
    {Query, Arity} when Arity =:= length(Ids) ->
      FormattedQuery = qfmt_i(Query, Ids),
      SetAllFunc = 
      fun ({match, _}) -> {ok, Values};
          ({not_found, _, _, _}) -> erlang:error(not_found)
      end,
      case ejsondb_crud:set(FormattedQuery, JsonStore, SetAllFunc, Funs, Opts) of
        {error, Err} -> {error, Err};
        {NewJsonStore, _Paths} -> {ok, NewJsonStore}
      end;
    _ -> {error, badarity}
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

modify_i(Op, Qid, Ids, Value, JsonStore, At, #ejsondb_schema{id_key = IdKey, funs = Funs, opts = Opts} = Schema)
  when Op == put orelse Op == create ->
  case qfind_i(Qid, Schema) of
    {error, Err} ->
      {error, Err};
    {Query, Arity} when Arity + 1 =:= length(Ids) ->
      PathIds = lists:droplast(Ids),
      Id = lists:last(Ids),
      FormattedQuery = qfmt_i(Query, PathIds),
      case ejsondb_crud:Op(FormattedQuery, IdKey, Id, Value, JsonStore, At, Funs, Opts) of
        {error, Err} -> {error, Err};
        NewJsonStore -> {ok, NewJsonStore}
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
    _ -> {error, query_not_found}
  end.