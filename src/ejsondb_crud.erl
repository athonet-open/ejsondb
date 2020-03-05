-module(ejsondb_crud).

-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-endif.

-include("ejsondb.hrl").

-export([
  get/4,
  set/5,
  get_by/6,

  create/8,
  put/8,
  upsert/8,
  delete/6, delete/4
]).

%% Jsonpath query
-type query() :: string().

%% Json encoded with jsx, jiffy with 'return_maps' option
-type json_node() :: ejsonpath:json_node().

%% Modifier and filter functions as in ejsonpath
-type modifier_funs() :: ejsonpath:jsonpath_funcsspecs().

%% Options as in ejsonpath
-type opts() :: [any()].

%% Transform function
-type transform_fun() :: ejsonpath:jsonpath_tr_func().

%% Result from json query
-type query_result() :: {[json_node()], [string()]}.

%% Result from json transform
-type transform_result() :: {json_node(), [string()]}.

%% Type used as key
-type id_value() :: string() | binary() | atom() | number().

%% Type used as position
-type insert_pos() :: '$head' | '$tail' | '$current' | number().

-export_type([
  query/0,
  json_node/0,
  modifier_funs/0,
  opts/0,
  transform_fun/0,
  query_result/0,
  transform_result/0,
  id_value/0,
  insert_pos/0
]).

%% @doc query json with ejsonpath.

-spec get(Query :: query(), Json :: json_node(), Funs :: modifier_funs(), Opts :: opts())
      -> query_result() | {error, term()}.

get(Query, Json, Funs, Opts) ->
  try ejsonpath:q(Query, Json, Funs, Opts)
  catch _:Err:_ -> {error, Err}
  end.


%% @doc transform json with ejsonpath.

-spec set(Query :: query(), Json :: json_node(), Fun :: transform_fun(), Funs :: modifier_funs(), Opts :: opts())
      -> transform_result() | {error, term()}.

set(Query, Json, Fun, Funs, Opts) ->
  try ejsonpath:tr(Query, Json, Fun, Funs, [handle_not_found | Opts])
  catch _:Err:_ -> {error, Err}
  end.


%% @doc query json by key with id.

-spec get_by(Query, Key, Id, Json, Funs, Opts)
      -> query_result() | {error, term()}
  when
  Query :: query(),
  Key :: ejsondb_id_key(),
  Id :: id_value(),
  Json :: json_node(),
  Funs :: modifier_funs(),
  Opts :: opts().

get_by(Query, Key, Id, Json, Funs, Opts) ->
  get(query_append_i(Query, Key, Id), Json, Funs, Opts).


%% @doc create element on json at position or fail if already exists.

-spec create(Query, Key, Id, Value, Json, At, Funs, Opts)
      -> query_result() | {error, term()}
  when
  Query :: query(),
  Key :: ejsondb_id_key(),
  Id :: id_value(),
  Value :: json_node(),
  Json :: json_node(),
  At :: insert_pos(),
  Funs :: modifier_funs(),
  Opts :: opts().

create(Query, Key, Id, Value, Json, At, Funs, Opts) ->
  try
    case get_list_i(Query, Json, Funs, Opts) of
      L when is_list(L) ->
        L1 = create_i(L, Key, Id, Value, At),
        update_list_i(Query, Json, L1, Funs, Opts);
      _ -> {error, not_found}
    end
  catch _:Err:_ -> {error, Err}
  end.


%% @doc put json element at position.
%% if not found create otherwise replace existing.

-spec put(Query, Key, Id, Value, Json, At, Funs, Opts)
      -> query_result() | {error, term()}
  when
  Query :: query(),
  Key :: ejsondb_id_key(),
  Id :: id_value(),
  Value :: json_node(),
  Json :: json_node(),
  At :: insert_pos(),
  Funs :: modifier_funs(),
  Opts :: opts().

put(Query, Key, Id, Value, Json, At, Funs, Opts) ->
  try
    case get_list_i(Query, Json, Funs, Opts) of
      L when is_list(L) ->
        L1 = put_i(L, Key, Id, Value, At),
        update_list_i(Query, Json, L1, Funs, Opts);
      _ -> {error, not_found}
    end
  catch _:Err:_ -> {error, Err}
  end.


%% @doc upsert json element at position.
%% if not found create otherwise merge with existing.

-spec upsert(Query, Key, Id, Value, Json, At, Funs, Opts)
      -> query_result() | {error, term()}
  when
  Query :: query(),
  Key :: ejsondb_id_key(),
  Id :: id_value(),
  Value :: json_node(),
  Json :: json_node(),
  At :: insert_pos(),
  Funs :: modifier_funs(),
  Opts :: opts().

upsert(Query, Key, Id, Value, Json, At, Funs, Opts) ->
  try
    case get_list_i(Query, Json, Funs, Opts) of
      L when is_list(L) ->
        L1 = upsert_i(L, Key, Id, Value, At),
        update_list_i(Query, Json, L1, Funs, Opts);
      _ -> {error, not_found}
    end
  catch _:Err:_ -> {error, Err}
  end.


%% @doc delete element by key with id.
%% if not found return success gracefully.

-spec delete(Query, Key, Id, Json, Funs, Opts) -> Result
  when
  Query :: query(),
  Key :: ejsondb_id_key(),
  Id :: id_value(),
  Json :: json_node(),
  Funs :: modifier_funs(),
  Opts :: opts(),
  Result :: json_node() | {error, term()}.

delete(Query, Key, Id, Json, Funs, Opts) ->
  delete(query_append_i(Query, Key, Id), Json, Funs, Opts).


%% @doc delete element by query.
%% if not found return success gracefully.

-spec delete(Query :: query(), Json :: json_node(), Funs :: modifier_funs(), Opts :: opts())
      -> json_node() | {error, term()}.

delete(Query, Json, Funs, Opts) ->
  try
    {Json1, _} = ejsonpath:tr(Query, Json, fun(_) -> delete end, Funs, [handle_not_found | Opts]),
    Json1
  catch
    _:not_found:_ -> Json;
    _:Err:_ -> {error, Err}
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_list_i(Query, Json, Funs, Opts) ->
  case ejsonpath:q(Query, Json, Funs, Opts) of
    {[L], [_]} -> L;
    _ -> erlang:error(not_found)
  end.

update_list_i(Query, Json, L, Funs, Opts) ->
  try
    case ejsonpath:tr(Query, Json, fun(_) -> {ok, L} end, Funs, Opts) of
      {NewJson, [_]} -> NewJson
    end
  catch _:_:_ -> erlang:error(not_found)
  end.

create_i(L, Key, Id, Value, At) ->
  case search_i(L, Key, Id) of
    false -> insert_list_i(At, maps:put(Key, Id, Value), L);
    _ -> erlang:error(already_exist)
  end.

put_i(L, Key, Id, Value, At) ->
  {Idx, L1} = drop_i(L, Key, Id),
  case At of
    '$current' ->
      insert_list_i(Idx, maps:put(Key, Id, Value), L1);
    _ -> insert_list_i(At, maps:put(Key, Id, Value), L1)
  end.

upsert_i(L, Key, Id, Value, At) ->
  case search_i(L, Key, Id) of
    false -> insert_list_i(At, maps:put(Key, Id, Value), L);
    {value, _} ->
      lists:map(fun(Element) ->
        case is_id_i(Key, Id, Element) of
          false -> Element;
          true -> maps:merge(Element, Value) % maybe recursive merge???
        end
                end, L)
  end.

drop_i(L, Key, Id) ->
  drop_i(L, Key, Id, {not_found, 0}, []).

drop_i([], _, _, {_, Idx}, Acc) -> {Idx, lists:reverse(Acc)};
drop_i([Element | Rest], Key, Id, {_, N} = Idx, Acc) ->
  case is_id_i(Key, Id, Element) of
    true -> drop_i(Rest, Key, Id, {found, N}, Acc);
    false -> drop_i(Rest, Key, Id, incr_i(Idx), [Element | Acc])
  end.

incr_i({not_found, Idx}) -> {not_found, Idx + 1};
incr_i({found, Idx}) -> {found, Idx}.

search_i(L, Key, Id) ->
  lists:search(fun(Element) -> is_id_i(Key, Id, Element) end, L).

is_id_i(Key, Id, Element) when is_map(Element) ->
  case maps:is_key(Key, Element) of
    false -> false;
    true ->
      maps:get(Key, Element, '$undefined') == Id
  end;

is_id_i(_, _, _) -> false.

insert_list_i('$head', Element, List) when is_list(List) ->
  [Element | List];
insert_list_i('$tail', Element, List) when is_list(List) ->
  List ++ [Element];
insert_list_i(Index, Element, [])
  when is_number(Index) ->
  insert_list_i('$head', Element, []);
insert_list_i(Idx, Element, List)
  when is_number(Idx), is_list(List), Idx >= 0, Idx >= length(List) ->
  insert_list_i('$tail', Element, List);
insert_list_i(Idx, Element, List)
  when is_number(Idx), is_list(List), Idx >= 0, Idx < length(List) ->
  lists:sublist(List, Idx) ++
    [Element] ++
    lists:nthtail(Idx, List);

insert_list_i(_, _, _) ->
  erlang:error(badarg).

query_append_i(Query, Key, Id)
  when (is_binary(Key) orelse is_atom(Key))
  andalso (is_list(Id) orelse is_binary(Id) orelse is_atom(Id)) ->
  lists:flatten(io_lib:format("~s[?(@.~s == '~s')]", [Query, Key, Id]));
query_append_i(Query, Key, Id)
  when (is_binary(Key) orelse is_atom(Key)) andalso is_number(Id) ->
  lists:flatten(io_lib:format("~s[?(@.~s == ~p)]", [Query, Key, Id]));
query_append_i(_, _, _) ->
  erlang:error(badarg).
