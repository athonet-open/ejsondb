-module(ejsondb_crud_tests).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").


get_test() ->
    % default key
    ?assertEqual({[#{}], ["$"]}, ejsondb_crud:get("$", #{})),
    
    ?assertEqual({[], []}, ejsondb_crud:get("$.value", #{})),
    ?assertEqual({[], []}, ejsondb_crud:get("$.outer.inner", #{})),
    ?assertEqual({[], []}, ejsondb_crud:get("$[1000][0:0]", [])),
    ?assertEqual({[], []}, ejsondb_crud:get("$[0:10]", [])),
    
    ?assertEqual({[10], ["$['value']"]}, ejsondb_crud:get("$.value", #{<<"value">> => 10})),

    ?assertEqual({error, not_implemented}, ejsondb_crud:get("$.value[0]", #{<<"value">> => 10})),    
    
    R1 = ejsondb_crud:get("$.value[@.id == 1]", #{<<"value">> => 10}),
    ?assertEqual(element(1, R1), error),

    ?assertEqual({[], []}, ejsondb_crud:get("$.value[?(@.id == 1)]", #{<<"value">> => 10})),
    ?assertEqual({[], []}, ejsondb_crud:get("$.value[?(@.id == 'oid')]", #{<<"value">> => 10})),
    ok.

set_test() ->
    ?assertEqual({#{}, ["$"]}, ejsondb_crud:set("$", #{}, fun(_) -> {ok, #{}} end)),
    
    % set value if match
    ?assertEqual({#{<<"value">> => yyy}, ["$['value']"]}, 
        ejsondb_crud:set("$.value", #{<<"value">> => xxx}, 
            fun({match, #{node := xxx}}) -> {ok, yyy} end)),
    % set value if not_found
    ?assertEqual({#{<<"other">> => xxx, <<"value">> => yyy}, ["$['value']"]}, 
        ejsondb_crud:set("$.value", #{<<"other">> => xxx}, 
            fun({not_found, _PathToBe, Key, #{node := M}}) -> {ok, maps:put(Key, yyy, M)} end)),

    % set in path not found
    ?assertEqual({error, not_implemented}, 
        ejsondb_crud:set("$.x.value", #{ <<"x">> => 10}, 
            fun(_) -> {ok, yyy} end)),
    % set inner value if match
    ?assertEqual({#{<<"x">> => #{<<"value">> => yyy}}, ["$['x']['value']"]}, 
        ejsondb_crud:set("$.x.value", #{ <<"x">> => #{<<"value">> => xxx}}, 
            fun({match, #{node := xxx}}) -> {ok, yyy} end)),
    % set inner value if not_found
    ?assertEqual({#{<<"x">> => #{<<"other">> => xxx, <<"value">> => yyy}}, ["$['x']['value']"]}, 
        ejsondb_crud:set("$.x.value", #{<<"x">> => #{<<"other">> => xxx}}, 
            fun({not_found, _PathToBe, Key, #{node := M}}) -> {ok, maps:put(Key, yyy, M)} end)),
    ok.

get_by_test() ->
    % default key
    ?assertEqual({[], []}, ejsondb_crud:get_by("$", <<"name">>, 0, #{})),
    ?assertEqual({[], []}, ejsondb_crud:get_by("$.items", <<"name">>, 0, #{})),

    ?assertEqual({[], []}, ejsondb_crud:get_by("$.items", <<"name">>, 0, #{<<"items">> => []})),
    ?assertEqual({[], []}, ejsondb_crud:get_by("$.items", <<"name">>, 0, #{<<"items">> => [1,2,3]})),
    ?assertEqual({[], []}, ejsondb_crud:get_by("$.items", <<"name">>, 0, #{<<"items">> => 10})),

    ?assertEqual({[], []}, ejsondb_crud:get_by("$.items", <<"name">>, 0, #{<<"items">> => [#{<<"id">> => 10}]})),
    ?assertEqual({[#{<<"name">> => 0}], [ "$['items'][0]"]}, ejsondb_crud:get_by("$.items", <<"name">>, 0, #{<<"items">> => [#{<<"name">> => 0}]})),
    ?assertEqual({[#{<<"name">> => <<"oid">>}], [ "$['items'][0]"]}, ejsondb_crud:get_by("$.items", <<"name">>, <<"oid">>, #{<<"items">> => [#{<<"name">> => <<"oid">>}]})),
    ?assertError(badarg, ejsondb_crud:get_by("$.items", #{}, <<"oid">>, #{<<"items">> => [#{<<"name">> => <<"oid">>}]})),
    ok.

create_test() ->
    % path doesn't exist
    ?assertEqual({error, not_found}, ejsondb_crud:create("$.inner", <<"id">>, 0, #{<<"value">> => 10}, #{})),
    ?assertEqual({error, not_found}, ejsondb_crud:create("$.inner", <<"id">>, 0, #{<<"value">> => 10}, #{<<"inner">> => 10})),

    % create or error already_exist
    JsonWithInner = #{ <<"inner">> => []},
    Json1 = ejsondb_crud:create("$.inner", <<"id">>, 0, #{<<"value">> => 10}, JsonWithInner),
    
    ?assertEqual(
        #{ <<"inner">> => [#{<<"id">> => 0, <<"value">> => 10}]},
        Json1
    ),
    
    ?assertEqual({error, already_exist}, ejsondb_crud:create("$.inner", <<"id">>, 0, #{<<"value">> => 10}, Json1)),

    % create at index
    Db = #{ <<"inner">> => [#{<<"id">> => 0}, #{<<"id">> => 1}, #{<<"id">> => 2}]},
    
    % head insert
    Json3 = ejsondb_crud:create("$.inner", <<"id">>, 10, #{<<"value">> => 10}, Db, '$head'),
    ?assertEqual(
        #{ <<"inner">> => [#{<<"id">> => 10, <<"value">> => 10}, #{<<"id">> => 0}, #{<<"id">> => 1}, #{<<"id">> => 2}]},
        Json3
    ),
    % tail insert
    Json4 = ejsondb_crud:create("$.inner", <<"id">>, 10, #{<<"value">> => 10}, Db, '$tail'),
    ?assertEqual(
        #{ <<"inner">> => [#{<<"id">> => 0}, #{<<"id">> => 1}, #{<<"id">> => 2}, #{<<"id">> => 10, <<"value">> => 10}]},
        Json4
    ),
    % index insert
    Json5 = ejsondb_crud:create("$.inner", <<"id">>, 10, #{<<"value">> => 10}, Db, 1),
    ?assertEqual(
        #{ <<"inner">> => [#{<<"id">> => 0}, #{<<"id">> => 10, <<"value">> => 10}, #{<<"id">> => 1}, #{<<"id">> => 2}]},
        Json5
    ),

    ok.

put_test() ->
    % path doesn't exist
    ?assertEqual({error, not_found}, ejsondb_crud:put("$.inner", <<"id">>, 0, #{<<"value">> => 10}, #{})),
    ?assertEqual({error, not_found}, ejsondb_crud:put("$.inner", <<"id">>, 0, #{<<"value">> => 10}, #{<<"inner">> => 10})),

    Json = #{<<"inner">> => []},
    Json1 = ejsondb_crud:put("$.inner", <<"id">>, 0, #{<<"value">> => 10}, Json),
    Json2 = ejsondb_crud:put("$.inner", <<"id">>, 0, #{<<"other">> => 10}, Json1),
    ?assertEqual(
        #{<<"inner">> => [#{<<"id">> => 0, <<"other">> => 10}]},
        Json2
    ),
    ok.

put_index_test() ->
    Json  = #{<<"inner">> => []},
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0}]}, ejsondb_crud:put("$.inner", <<"id">>, 0, #{}, Json)),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0}]}, ejsondb_crud:put("$.inner", <<"id">>, 0, #{}, Json, '$head')),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0}]}, ejsondb_crud:put("$.inner", <<"id">>, 0, #{}, Json, '$tail')),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0}]}, ejsondb_crud:put("$.inner", <<"id">>, 0, #{}, Json, 0)),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0}]}, ejsondb_crud:put("$.inner", <<"id">>, 0, #{}, Json, 1)),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0}]}, ejsondb_crud:put("$.inner", <<"id">>, 0, #{}, Json, 2)),

    Json1 = #{<<"inner">> => [
        #{<<"id">> => 0, <<"value">> => 10},
        #{<<"id">> => 1, <<"value">> => 10},
        #{<<"id">> => 2, <<"value">> => 10}
    ]},
    
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 2, <<"value">> => 10}, #{<<"id">> => 0}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 0, #{}, Json1)),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0}, #{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 2, <<"value">> => 10}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 0, #{}, Json1, '$head')),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 2, <<"value">> => 10}, #{<<"id">> => 0}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 0, #{}, Json1, '$tail')),
    
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0}, #{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 2, <<"value">> => 10}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 0, #{}, Json1, 0)),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 0}, #{<<"id">> => 2, <<"value">> => 10}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 0, #{}, Json1, 1)),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 2, <<"value">> => 10}, #{<<"id">> => 0}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 0, #{}, Json1, 2)),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 2, <<"value">> => 10}, #{<<"id">> => 0}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 0, #{}, Json1, 3)),

    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0, <<"value">> => 10}, #{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 2, <<"value">> => 10}, #{<<"id">> => 10}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 10, #{}, Json1)),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 10}, #{<<"id">> => 0, <<"value">> => 10}, #{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 2, <<"value">> => 10}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 10, #{}, Json1, '$head')),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0, <<"value">> => 10}, #{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 2, <<"value">> => 10}, #{<<"id">> => 10}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 10, #{}, Json1, '$tail')),

    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 10}, #{<<"id">> => 0, <<"value">> => 10}, #{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 2, <<"value">> => 10}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 10, #{}, Json1, 0)),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0, <<"value">> => 10}, #{<<"id">> => 10}, #{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 2, <<"value">> => 10}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 10, #{}, Json1, 1)),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0, <<"value">> => 10}, #{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 10}, #{<<"id">> => 2, <<"value">> => 10}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 10, #{}, Json1, 2)),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0, <<"value">> => 10}, #{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 2, <<"value">> => 10}, #{<<"id">> => 10}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 10, #{}, Json1, 3)),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0, <<"value">> => 10}, #{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 2, <<"value">> => 10}, #{<<"id">> => 10}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 10, #{}, Json1, 4)),

    % store at previous index
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0}, #{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 2, <<"value">> => 10}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 0, #{}, Json1, '$current')),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0, <<"value">> => 10}, #{<<"id">> => 1}, #{<<"id">> => 2, <<"value">> => 10}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 1, #{}, Json1, '$current')),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0, <<"value">> => 10}, #{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 2}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 2, #{}, Json1, '$current')),
    ?assertEqual(#{<<"inner">> => [#{<<"id">> => 0, <<"value">> => 10}, #{<<"id">> => 1, <<"value">> => 10}, #{<<"id">> => 2, <<"value">> => 10}, #{<<"id">> => 10}]}, 
        ejsondb_crud:put("$.inner", <<"id">>, 10, #{}, Json1, '$current')), % missing element will append
    
    ok.

upsert_test() ->
    % path doesn't exist
    ?assertEqual({error, not_found}, ejsondb_crud:upsert("$.inner", <<"id">>, 0, #{<<"value">> => 10}, #{})),
    ?assertEqual({error, not_found}, ejsondb_crud:upsert("$.inner", <<"id">>, 0, #{<<"value">> => 10}, #{<<"inner">> => 10})),

    Json = #{<<"inner">> => []},
    Json1 = ejsondb_crud:upsert("$.inner", <<"id">>, 0, #{<<"value">> => 10}, Json),
    Json2 = ejsondb_crud:upsert("$.inner", <<"id">>, 0, #{<<"value">> => 100, <<"other">> => 10}, Json1),
    ?assertEqual(
        #{<<"inner">> => [#{<<"id">> => 0, <<"value">> => 100, <<"other">> => 10}]},
        Json2
    ),
    
    Json3 = #{<<"inner">> => [#{<<"id">> => 10}]},
    Json4 = ejsondb_crud:upsert("$.inner", <<"id">>, 0, #{<<"value">> => 10}, Json3),
    Json5 = ejsondb_crud:upsert("$.inner", <<"id">>, 0, #{<<"value">> => 100, <<"other">> => 10}, Json4),
    ?assertEqual(
        #{<<"inner">> => [#{<<"id">> => 10}, #{<<"id">> => 0, <<"value">> => 100, <<"other">> => 10}]},
        Json5
    ),

    ok.

delete_test() ->
    % path doesn't exist
    ?assertEqual(#{}, ejsondb_crud:delete("$.inner", <<"id">>, 0, #{})),
    ?assertEqual(#{<<"inner">> => 10}, ejsondb_crud:delete("$.inner", <<"id">>, 0, #{<<"inner">> => 10})),
    ?assertEqual({error, not_implemented}, ejsondb_crud:delete("$.inner.value", <<"id">>, 0, #{<<"inner">> => 10})),
    
    Json = #{ <<"inner">> => [#{<<"id">> => 0, <<"value">> => 10}]},
    ?assertEqual(#{<<"inner">> => []}, ejsondb_crud:delete("$.inner[?(@.id == 0)]", Json)),
    ?assertEqual(#{<<"inner">> => []}, ejsondb_crud:delete("$.inner", <<"id">>, 0, Json)),
    ok.


update_list_i_test() ->
    ?assertError(not_found, ejsondb_crud:update_list_i("$.inner", #{}, [])),
    ?assertError(not_found, ejsondb_crud:update_list_i("$.inner.inner", #{}, [])),

    ?assertEqual(#{<<"inner">> => [y, y, y]}, ejsondb_crud:update_list_i("$.inner", #{<<"inner">> => [x, x, x]}, [y, y, y])),
    ok. 


insert_list_i_test() ->
    ?assertError(badarg, ejsondb_crud:insert_list_i('$head', x, y)),

    ?assertEqual([x, y], ejsondb_crud:insert_list_i('$head', x, [y])),
    ?assertEqual([x, y], ejsondb_crud:insert_list_i(0, x, [y])),

    ?assertEqual([y, x], ejsondb_crud:insert_list_i('$tail', x, [y])),
    ?assertEqual([y, x], ejsondb_crud:insert_list_i(1, x, [y])),
    ?assertEqual([y, x], ejsondb_crud:insert_list_i(2, x, [y])),
    ok.

is_id_i_test() ->
    ?assertEqual(false, ejsondb_crud:is_id_i(a, b, c)),
    ?assertEqual(false, ejsondb_crud:is_id_i(<<"a">>, 0, #{})),
    ?assertEqual(false, ejsondb_crud:is_id_i(<<"a">>, 0, #{<<"a">> => 10})),
    ?assertEqual(true, ejsondb_crud:is_id_i(<<"a">>, 0, #{<<"a">> => 0})),
    
    ok.