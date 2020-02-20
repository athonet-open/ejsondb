-module(ejsondb_tests).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("ejsondb.hrl").

new_test() ->
    #ejsondb_schema{qtab = [], id_key = <<"id">>} = ejsondb:new(<<"id">>, []).

get_test() ->
    Schema = ejsondb:new(<<"id">>, qtab()),
    ?assertEqual({error, query_not_found}, ejsondb:get(d, [], storage(), Schema)),
    ?assertEqual({error, query_not_found}, ejsondb:get(d, [0], storage(), Schema)),
    ?assertEqual({error, query_not_found}, ejsondb:get(d, [0,1], storage(), Schema)),
    
    % level.1
    ?assertEqual({error, badarity}, ejsondb:get(a, [], storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:get(a, [1, 0], storage(), Schema)),
    ?assertEqual({error, not_found}, ejsondb:get(a, [0], #{}, Schema)),
    ?assertEqual({error, not_found}, ejsondb:get(a, [2], storage(), Schema)),
    
    ?assertEqual({ok, #{<<"id">> => 0, <<"value">> => xxx}}, ejsondb:get(a, [0], storage(), Schema)),

    % level.2
    ?assertEqual({error, badarity}, ejsondb:get(b, [], storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:get(b, [0], storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:get(b, [1, 2, 3], storage(), Schema)),

    ?assertEqual({error, not_found}, ejsondb:get(b, [0, 0], storage(), Schema)),
    ?assertEqual({error, not_found}, ejsondb:get(b, [1, 2], storage(), Schema)),

    ?assertEqual({ok, #{<<"id">> => 0, <<"value">> => yyy}}, ejsondb:get(b, [1, 0], storage(), Schema)),
    
    % level.3
    ?assertEqual({error, badarity}, ejsondb:get(c, [], storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:get(c, [1], storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:get(c, [1, 1], storage(), Schema)),

    ?assertEqual({error, not_found}, ejsondb:get(c, [1, 1, 2], storage(), Schema)),

    ?assertEqual({ok, #{<<"id">> => 0, <<"value">> => zzz}}, ejsondb:get(c, [1, 1, 0], storage(), Schema)),
    ok.

set_test() -> 
    Schema = ejsondb:new(<<"id">>, qtab()),
    ?assertEqual({error, query_not_found}, ejsondb:set(d, [], #{}, storage(), Schema)),
    ?assertEqual({error, query_not_found}, ejsondb:set(d, [0], #{}, storage(), Schema)),
    ?assertEqual({error, query_not_found}, ejsondb:set(d, [0,1], #{}, storage(), Schema)),

    % level.1
    ?assertEqual({error, badarity}, ejsondb:set(a, [], #{}, storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:set(a, [0,1], #{}, storage(), Schema)),

    % override
    ?assertEqual({ok, #{<<"a">> => [
                #{<<"id">> => 0,<<"value">> => xxx},
                #{<<"id">> => 1,<<"new_value">> => abc}]}}, ejsondb:set(a, [1], #{<<"new_value">> => abc}, storage(), Schema)),

    % put new element
    ?assertEqual({ok, #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx},
            #{ <<"id">> => 1, <<"b">> => [
                #{<<"id">> => 0, <<"value">> => yyy},
                #{ <<"id">> => 1, <<"c">> => [
                    #{<<"id">> => 0, <<"value">> => zzz},
                    #{<<"id">> => 1, <<"value">> => xyz}
                ]}
            ]},
            #{ <<"id">> => 10, <<"new_value">> => abc}
        ]
    }}, ejsondb:set(a, [10], #{<<"new_value">> => abc}, storage(), Schema)),
    
    % level.2
    ?assertEqual({error, badarity}, ejsondb:set(b, [], #{}, storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:set(b, [0,1,1], #{}, storage(), Schema)),
    ?assertEqual({error, not_found}, ejsondb:set(b, [0, 0], #{}, storage(), Schema)),

    % override
    ?assertEqual({ok, #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx},
            #{ <<"id">> => 1, <<"b">> => [
                #{<<"id">> => 0, <<"value">> => yyy},
                #{<<"id">> => 1, <<"new_value">> => abc}
            ]}
        ]
    }}, ejsondb:set(b, [1, 1], #{<<"new_value">> => abc}, storage(), Schema)),
    % override at current index
    ?assertEqual({ok, #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx},
            #{ <<"id">> => 1, <<"b">> => [
                #{<<"id">> => 0, <<"new_value">> => abc},
                #{ <<"id">> => 1, <<"c">> => [
                    #{<<"id">> => 0, <<"value">> => zzz},
                    #{<<"id">> => 1, <<"value">> => xyz}
                ]}
            ]}
        ]
    }}, ejsondb:set(b, [1, 0], #{<<"new_value">> => abc}, storage(), Schema)),

    % put new element
    ?assertEqual({ok, #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx},
            #{ <<"id">> => 1, <<"b">> => [
                #{<<"id">> => 0, <<"value">> => yyy},
                #{ <<"id">> => 1, <<"c">> => [
                    #{<<"id">> => 0, <<"value">> => zzz},
                    #{<<"id">> => 1, <<"value">> => xyz}
                ]},
                #{<<"id">> => 10, <<"new_value">> => abc}
            ]}
        ]
    }}, ejsondb:set(b, [1, 10], #{<<"new_value">> => abc}, storage(), Schema)),

    % level.3
    ?assertEqual({error, badarity}, ejsondb:set(c, [], #{}, storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:set(c, [0, 1], #{}, storage(), Schema)),
    ?assertEqual({error, not_found}, ejsondb:set(c, [1, 0, 0], #{}, storage(), Schema)),

    % override
    ?assertEqual({ok, #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx},
            #{ <<"id">> => 1, <<"b">> => [
                #{<<"id">> => 0, <<"value">> => yyy},
                #{ <<"id">> => 1, <<"c">> => [
                    #{<<"id">> => 0, <<"new_value">> => xxx},
                    #{<<"id">> => 1, <<"value">> => xyz}
                ]}
            ]}
        ]
    }}, ejsondb:set(c, [1, 1, 0], #{<<"new_value">> => xxx}, storage(), Schema)),
    % put new element
    ?assertEqual({ok, #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx},
            #{ <<"id">> => 1, <<"b">> => [
                #{<<"id">> => 0, <<"value">> => yyy},
                #{ <<"id">> => 1, <<"c">> => [
                    #{<<"id">> => 0, <<"value">> => zzz},
                    #{<<"id">> => 1, <<"value">> => xyz},
                    #{<<"id">> => 10, <<"new_value">> => xxx}
                ]}
            ]}
        ]
    }}, ejsondb:set(c, [1, 1, 10], #{<<"new_value">> => xxx}, storage(), Schema)),
    ok.

add_test() -> 
    Schema = ejsondb:new(<<"id">>, qtab()),
    ?assertEqual({error, query_not_found}, ejsondb:add(d, [], #{}, storage(), Schema)),
    ?assertEqual({error, query_not_found}, ejsondb:add(d, [0], #{}, storage(), Schema)),
    ?assertEqual({error, query_not_found}, ejsondb:add(d, [0,1], #{}, storage(), Schema)),

    % level.1
    ?assertEqual({error, badarity}, ejsondb:add(a, [], #{}, storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:add(a, [0,1], #{}, storage(), Schema)),

    % override
    ?assertEqual({error, already_exist}, ejsondb:add(a, [1], #{<<"new_value">> => abc}, storage(), Schema)),

    % put new element
    ?assertEqual({ok, #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx},
            #{ <<"id">> => 1, <<"b">> => [
                #{<<"id">> => 0, <<"value">> => yyy},
                #{ <<"id">> => 1, <<"c">> => [
                    #{<<"id">> => 0, <<"value">> => zzz},
                    #{<<"id">> => 1, <<"value">> => xyz}
                ]}
            ]},
            #{ <<"id">> => 10, <<"new_value">> => abc}
        ]
    }}, ejsondb:add(a, [10], #{<<"new_value">> => abc}, storage(), Schema)),
    
    % level.2
    ?assertEqual({error, badarity}, ejsondb:add(b, [], #{}, storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:add(b, [0,1,1], #{}, storage(), Schema)),
    ?assertEqual({error, not_found}, ejsondb:add(b, [0, 0], #{}, storage(), Schema)),

    % override
    ?assertEqual({error, already_exist}, ejsondb:add(b, [1, 1], #{<<"new_value">> => abc}, storage(), Schema)),
    % override at current index
    ?assertEqual({error, already_exist}, ejsondb:add(b, [1, 0], #{<<"new_value">> => abc}, storage(), Schema)),

    % put new element
    ?assertEqual({ok, #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx},
            #{ <<"id">> => 1, <<"b">> => [
                #{<<"id">> => 0, <<"value">> => yyy},
                #{ <<"id">> => 1, <<"c">> => [
                    #{<<"id">> => 0, <<"value">> => zzz},
                    #{<<"id">> => 1, <<"value">> => xyz}
                ]},
                #{<<"id">> => 10, <<"new_value">> => abc}
            ]}
        ]
    }}, ejsondb:add(b, [1, 10], #{<<"new_value">> => abc}, storage(), Schema)),

    % level.3
    ?assertEqual({error, badarity}, ejsondb:add(c, [], #{}, storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:add(c, [0, 1], #{}, storage(), Schema)),
    ?assertEqual({error, not_found}, ejsondb:add(c, [1, 0, 0], #{}, storage(), Schema)),

    % override
    ?assertEqual({error, already_exist}, ejsondb:add(c, [1, 1, 0], #{<<"new_value">> => xxx}, storage(), Schema)),
    % put new element
    ?assertEqual({ok, #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx},
            #{ <<"id">> => 1, <<"b">> => [
                #{<<"id">> => 0, <<"value">> => yyy},
                #{ <<"id">> => 1, <<"c">> => [
                    #{<<"id">> => 0, <<"value">> => zzz},
                    #{<<"id">> => 1, <<"value">> => xyz},
                    #{<<"id">> => 10, <<"new_value">> => xxx}
                ]}
            ]}
        ]
    }}, ejsondb:add(c, [1, 1, 10], #{<<"new_value">> => xxx}, storage(), Schema)),
    ok.

add_at_test() -> 
    Schema = ejsondb:new(<<"id">>, qtab()),
    ?assertEqual({error, query_not_found}, ejsondb:add_at(d, [], #{}, storage(), '$head', Schema)),
    ?assertEqual({error, query_not_found}, ejsondb:add_at(d, [0], #{}, storage(), '$head', Schema)),
    ?assertEqual({error, query_not_found}, ejsondb:add_at(d, [0,1], #{}, storage(), '$head', Schema)),
    ?assertEqual({error, badarg}, ejsondb:add_at(a, [10], #{}, storage(), '$current', Schema)),
    
    % override
    ?assertEqual({error, already_exist}, ejsondb:add_at(c, [1, 1, 0], #{<<"new_value">> => xxx}, storage(), '$head', Schema)),

    % put new element (head)
    ?assertEqual({ok, #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx},
            #{ <<"id">> => 1, <<"b">> => [
                #{<<"id">> => 0, <<"value">> => yyy},
                #{ <<"id">> => 1, <<"c">> => [
                    #{<<"id">> => 10, <<"new_value">> => xxx},
                    #{<<"id">> => 0, <<"value">> => zzz},
                    #{<<"id">> => 1, <<"value">> => xyz}
                ]}
            ]}
        ]
    }}, ejsondb:add_at(c, [1, 1, 10], #{<<"new_value">> => xxx}, storage(), '$head', Schema)),
    % put new element (head)
    ?assertEqual({ok, #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx},
            #{ <<"id">> => 1, <<"b">> => [
                #{<<"id">> => 0, <<"value">> => yyy},
                #{ <<"id">> => 1, <<"c">> => [
                    #{<<"id">> => 0, <<"value">> => zzz},
                    #{<<"id">> => 1, <<"value">> => xyz},
                    #{<<"id">> => 10, <<"new_value">> => xxx}
                ]}
            ]}
        ]
    }}, ejsondb:add_at(c, [1, 1, 10], #{<<"new_value">> => xxx}, storage(), '$tail', Schema)),

    ?assertEqual({ok, #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx},
            #{ <<"id">> => 1, <<"b">> => [
                #{<<"id">> => 0, <<"value">> => yyy},
                #{ <<"id">> => 1, <<"c">> => [
                    #{<<"id">> => 0, <<"value">> => zzz},
                    #{<<"id">> => 10, <<"new_value">> => xxx},
                    #{<<"id">> => 1, <<"value">> => xyz}
                ]}
            ]}
        ]
    }}, ejsondb:add_at(c, [1, 1, 10], #{<<"new_value">> => xxx}, storage(), 1, Schema)),
    ok.

delete_test() -> 
    Schema = ejsondb:new(<<"id">>, qtab()),
    ?assertEqual({error, query_not_found}, ejsondb:delete(d, [], storage(), Schema)),
    ?assertEqual({error, query_not_found}, ejsondb:delete(d, [0], storage(), Schema)),
    ?assertEqual({error, query_not_found}, ejsondb:delete(d, [0,1], storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:delete(a, [0, 10], storage(), Schema)),
    
    % doesn't exist ok
    ?assertEqual({ok, storage()}, ejsondb:delete(a, [10], storage(), Schema)),
    % path doesn't exist ok
    ?assertEqual({ok, storage()}, ejsondb:delete(c, [1, 10, 0], storage(), Schema)),
    
    ?assertEqual({ok, #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx}
        ]
    }}, ejsondb:delete(a, [1], storage(), Schema)),
    ?assertEqual({ok, #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx},
            #{ <<"id">> => 1, <<"b">> => [
                #{<<"id">> => 0, <<"value">> => yyy},
                #{ <<"id">> => 1, <<"c">> => [
                    #{<<"id">> => 1, <<"value">> => xyz}
                ]}
            ]}
        ]
    }}, ejsondb:delete(c, [1, 1, 0], storage(), Schema)),
    ok.

get_all_test()  ->
    Schema = ejsondb:new(<<"id">>, qtab()),
    ?assertEqual({error, query_not_found}, ejsondb:get_all(d, [], storage(), Schema)),
    ?assertEqual({error, query_not_found}, ejsondb:get_all(d, [0], storage(), Schema)),
    ?assertEqual({error, query_not_found}, ejsondb:get_all(d, [0,1], storage(), Schema)),
    
    % level.1
    ?assertEqual({error, badarity}, ejsondb:get_all(a, [0], storage(), Schema)),
    ?assertEqual({error, not_found}, ejsondb:get_all(a, [], #{}, Schema)),

    ?assertEqual({ok, maps:get(<<"a">>, storage())}, ejsondb:get_all(a, [], storage(), Schema)),
    
    % level.2
    ?assertEqual({error, badarity}, ejsondb:get_all(b, [], storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:get_all(b, [0, 1], storage(), Schema)),
    ?assertEqual({error, not_found}, ejsondb:get_all(b, [0], storage(), Schema)),

    ?assertEqual({ok, [
                #{<<"id">> => 0, <<"value">> => yyy},
                #{ <<"id">> => 1, <<"c">> => [
                    #{<<"id">> => 0, <<"value">> => zzz},
                    #{<<"id">> => 1, <<"value">> => xyz}
                ]}]}, ejsondb:get_all(b, [1], storage(), Schema)),

    % level.3
    ?assertEqual({error, badarity}, ejsondb:get_all(c, [], storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:get_all(c, [1], storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:get_all(c, [1, 1, 0], storage(), Schema)),
    
    ?assertEqual({error, not_found}, ejsondb:get_all(c, [1, 0], storage(), Schema)),
    
    ?assertEqual({ok, [
                    #{<<"id">> => 0, <<"value">> => zzz},
                    #{<<"id">> => 1, <<"value">> => xyz}
                ]}, ejsondb:get_all(c, [1, 1], storage(), Schema)),
    ok.

set_all_test() ->
    Schema = ejsondb:new(<<"id">>, qtab()),
    ?assertEqual({error, query_not_found}, ejsondb:set_all(d, [], [], storage(), Schema)),
    ?assertEqual({error, query_not_found}, ejsondb:set_all(d, [0], [], storage(), Schema)),
    ?assertEqual({error, query_not_found}, ejsondb:set_all(d, [0,1], [], storage(), Schema)),
    
    % level.1
    ?assertEqual({error, badarity}, ejsondb:set_all(a, [0], [], storage(), Schema)),
    ?assertEqual({error, not_found}, ejsondb:set_all(a, [], [], #{}, Schema)),

    ?assertEqual({ok, #{<<"a">> => []}}, ejsondb:set_all(a, [], [], storage(), Schema)),
    ?assertEqual({ok, #{<<"a">> => [1,2,3]}}, ejsondb:set_all(a, [], [1,2,3], storage(), Schema)),
    
    % level.2
    ?assertEqual({error, badarity}, ejsondb:set_all(b, [], [], storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:set_all(b, [0, 1], [], storage(), Schema)),
    ?assertEqual({error, not_found}, ejsondb:set_all(b, [0], [], storage(), Schema)),

    ?assertEqual({ok, #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx},
            #{ <<"id">> => 1, <<"b">> => []}
        ]
    }}, ejsondb:set_all(b, [1], [], storage(), Schema)),

    % level.3
    ?assertEqual({error, badarity}, ejsondb:set_all(c, [], [], storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:set_all(c, [1], [], storage(), Schema)),
    ?assertEqual({error, badarity}, ejsondb:set_all(c, [1, 1, 0], [], storage(), Schema)),
    
    ?assertEqual({error, not_found}, ejsondb:set_all(c, [1, 0], [], storage(), Schema)),
    
    ?assertEqual({ok, #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx},
            #{ <<"id">> => 1, <<"b">> => [
                #{<<"id">> => 0, <<"value">> => yyy},
                #{ <<"id">> => 1, <<"c">> => []}
            ]}
        ]
    }}, ejsondb:set_all(c, [1, 1], [], storage(), Schema)),
    ok.

qfind_i_test() ->
    Schema = ejsondb:new(<<"id">>, qtab()),
    ?assertEqual({"$.a", 0}, ejsondb:qfind_i(a, Schema)),
    ?assertEqual({error, query_not_found}, ejsondb:qfind_i(d, Schema)),
    ok.

modify_i_test() ->
    ?assertError(bad_operation, ejsondb:modify_i(something, 0, 0, 0, 0, 0, #ejsondb_schema{})),
    ok.

invalid_query_test() ->
    Schema = ejsondb:new(<<"id">>, [{a, "$[??]", 0}]),
    ?assertEqual(error, element(1, ejsondb:get(a, [0], storage(), Schema))),
    ?assertEqual(error, element(1, ejsondb:delete(a, [0], storage(), Schema))),
    ?assertEqual(error, element(1, ejsondb:get_all(a, [], storage(), Schema))),
    ok.

keyacces_atom_test() ->
    Tabs = [
        {a, "$.a", 0},
        {b, "$.a[?(@.id == ~p)].b", 1}
    ],

    Json = #{
        a => [
            #{id => 0, value => xxx},
            #{id => 1, value => yyy},
            #{id => 2, b => [
                #{id => 0, count => 10},
                #{id => 1, count => 12}
            ]}
        ]
    },

    Schema = ejsondb:new(id, Tabs, #{}, [{keyaccess, fun (X) -> erlang:binary_to_atom(X, utf8) end}]),

    %% GET
    ?assertEqual(
        {ok, #{id => 0, value => xxx}},
        ejsondb:get(a, [0], Json, Schema)
    ),
    ?assertEqual(
        {ok, #{id => 1,value => yyy}},
        ejsondb:get(a, [1], Json, Schema)
    ),
    ?assertEqual(
        {ok, #{count => 10, id => 0}},
        ejsondb:get(b, [2,0], Json, Schema)
    ),
    ?assertEqual(
        {ok, #{count => 12, id => 1}},
        ejsondb:get(b, [2,1], Json, Schema)
    ),
    ?assertEqual(
        {error, not_found},
        ejsondb:get(b, [2,2], Json, Schema)
    ),

    %% ADD
    ?assertEqual(
        {error, already_exist},
        ejsondb:add(b, [2,1], #{}, Json, Schema)
    ),

    {ok, Json1} = ejsondb:add(b, [2,2], #{n => 100}, Json, Schema),
    ?assertEqual(
        {ok, #{id => 2, n => 100}},
        ejsondb:get(b, [2,2], Json1, Schema)
    ),

    %% SET
    {ok, Json2} = ejsondb:set(b, [2,10], #{n => 1000}, Json1, Schema),
    ?assertEqual(
        {ok, #{id => 10, n => 1000}},
        ejsondb:get(b, [2,10], Json2, Schema)
    ),

    %% DELETE
    {ok, Json3} = ejsondb:delete(b, [2, 10], Json2, Schema),
    ?assertEqual(
        {error, not_found},
        ejsondb:get(b, [2, 10], Json3, Schema)
    ),
    %% SET_ALL/GET_ALL
    {ok, Json4} = ejsondb:set_all(b, [2], [], Json3, Schema),
    ?assertEqual({ok, []}, ejsondb:get_all(b, [2], Json4, Schema)),

    {ok, Json5} = ejsondb:set_all(a, [], [], Json3, Schema),
    ?assertEqual(#{a => []}, Json5),
    ?assertEqual({ok, []}, ejsondb:get_all(a, [], Json5, Schema)),

    ok.

qtab() ->
    [
        {a, "$.a", 0},
        {b, "$.a[?(@.id == ~p)].b", 1},
        {c, "$.a[?(@.id == ~p)].b[?(@.id == ~p)].c", 2}
    ].

storage() ->
    #{
        <<"a">> => [
            #{ <<"id">> => 0, <<"value">> => xxx},
            #{ <<"id">> => 1, <<"b">> => [
                #{<<"id">> => 0, <<"value">> => yyy},
                #{ <<"id">> => 1, <<"c">> => [
                    #{<<"id">> => 0, <<"value">> => zzz},
                    #{<<"id">> => 1, <<"value">> => xyz}
                ]}
            ]}
        ]
    }.

