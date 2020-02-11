-module(ejsondb_crud).

-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-endif.

-export([
  get/2, set/3, 
  get_by/4,

  create/5, create/6,
  put/5, put/6,
  upsert/5, upsert/6,
  delete/4, delete/2
]).

get(Query, Json) ->
  try ejsonpath:q(Query, Json, #{}, [])
  catch _:Err:_ -> {error, Err}
  end.

set(Query, Json, Func) ->
  try ejsonpath:tr(Query, Json, Func, #{}, [handle_not_found])
  catch _:Err:_ -> {error, Err}
  end.

get_by(Query, Key, Id, Json) ->
  get(query_append_i(Query, Key, Id), Json).

create(Query, Key, Id, Value, Json) ->
  create(Query, Key, Id, Value, Json, '$tail').

create(Query, Key, Id, Value, Json, At) ->
  try 
    case get_list_i(Query, Json) of
      L when is_list(L) ->
        L1 = create_i(L, Key, Id, Value, At),
        update_list_i(Query, Json, L1);
      _ -> {error, not_found}
    end
  catch _:Err:_ -> {error, Err}
  end.

put(Query, Key, Id, Value, Json) ->
  put(Query, Key, Id, Value, Json, '$tail').
put(Query, Key, Id, Value, Json, At) ->
  try
    case get_list_i(Query, Json) of
      L when is_list(L) ->
        L1 = put_i(L, Key, Id, Value, At),
        update_list_i(Query, Json, L1);
      _ -> {error, not_found}
    end
  catch _:Err:_ -> {error, Err}
  end.

upsert(Query, Key, Id, Value, Json) ->
  upsert(Query, Key, Id, Value, Json, '$tail').
upsert(Query, Key, Id, Value, Json, At) ->
  try
    case get_list_i(Query, Json) of
      L when is_list(L) ->
        L1 = upsert_i(L, Key, Id, Value, At),
        update_list_i(Query, Json, L1);
      _ -> {error, not_found}
    end
  catch _:Err:_ -> {error, Err}
  end.

delete(Query, Key, Id, Json) ->
  delete(query_append_i(Query, Key, Id), Json).
delete(Query, Json) ->
  try
    {Json1, _} = ejsonpath:tr(Query, Json, fun(_) -> delete end, #{}, [handle_not_found]),
    Json1
  catch
    _:not_found:_ -> Json;
    _:Err:_ -> {error, Err}
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_list_i(Query, Json) ->
  case ejsonpath:q(Query, Json, #{}, []) of
    {[L], [_]} -> L;
    _ -> erlang:error(not_found)
  end.

update_list_i(Query, Json, L) ->
  try
    case ejsonpath:tr(Query, Json, fun(_) -> {ok, L} end, #{}, []) of
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
    _ ->  insert_list_i(At, maps:put(Key, Id, Value), L1)
  end.

upsert_i(L, Key, Id, Value, At) ->
  case search_i(L, Key, Id) of
    false -> insert_list_i(At, maps:put(Key, Id, Value), L);
    {value, _} -> 
      lists:map(fun (Element) ->
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
    false -> drop_i(Rest, Key, Id, incr_i(Idx), [Element|Acc])
  end.

incr_i({not_found, Idx}) -> {not_found, Idx+1};
incr_i({found, Idx}) -> {found, Idx}.

search_i(L, Key, Id) ->
  lists:search(fun (Element) -> is_id_i(Key, Id, Element) end, L).

is_id_i(Key, Id, Element) when is_map(Element) ->
  case maps:is_key(Key, Element) of
    false -> false;
    true -> 
      maps:get(Key, Element, '$undefined') == Id
  end;

is_id_i(_,_,_) -> false.

insert_list_i('$head', Element, List) when is_list(List) ->
  [Element|List];
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
  [ Element ] ++ 
  lists:nthtail(Idx, List);

insert_list_i(_, _, _) ->
  erlang:error(badarg).

query_append_i(Query, Key, Id) 
  when is_binary(Key) andalso (is_list(Id) orelse is_binary(Id) orelse is_atom(Id))->
  lists:flatten(io_lib:format("~s[?(@.~s == '~s')]", [Query, Key, Id]));
query_append_i(Query, Key, Id) 
  when is_binary(Key) andalso is_number(Id) ->
  lists:flatten(io_lib:format("~s[?(@.~s == ~p)]", [Query, Key, Id]));
query_append_i(_, _, _) ->
   erlang:error(badarg).
