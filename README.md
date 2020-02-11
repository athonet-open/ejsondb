ejsondb
=======

### usage

```erl
Tabs = [
    {a, "$.a", 0},
    {b, "$.a[?(@.id == ~p)].b", 1}
].

Json = #{
    <<"a">> => [
        #{<<"id">> => 0, <<"value">> => xxx},
        #{<<"id">> => 1, <<"value">> => yyy},
        #{<<"id">> => 2, <<"b">> => [
            #{<<"id">> => 0, <<"count">> => 10},
            #{<<"id">> => 1, <<"count">> => 12}
        ]}
    ]
}.

Schema = ejsondb:new(<<"id">>, Tabs).

%% GET
ejsondb:get(a, [0], Json, Schema).
%> #{<<"id">> => 0,<<"value">> => xxx}
ejsondb:get(a, [1], Json, Schema).
%> #{<<"id">> => 1,<<"value">> => yyy}

ejsondb:get(b, [2,0], Json, Schema).  
%> #{<<"count">> => 10,<<"id">> => 0}
ejsondb:get(b, [2,1], Json, Schema).
%> #{<<"count">> => 12,<<"id">> => 1}
ejsondb:get(b, [2,2], Json, Schema).
%>{error,not_found}

%% ADD
ejsondb:add(b, [2,1], #{}, Json, Schema).
%> {error,already_exist}
Json1 = ejsondb:add(b, [2,2], #{<<"n">> => 100}, Json, Schema).
ejsondb:get(b, [2,2], Json1, Schema).
%> #{<<"id">> => 2,<<"n">> => 100}

%% SET
Json2 = ejsondb:set(b, [2,10], #{<<"n">> => 1000}, Json1, Schema).
ejsondb:get(b, [2,10], Json2, Schema).
%> #{<<"id">> => 10,<<"n">> => 1000}

%% DELETE
Json3 = ejsondb:delete(b, [2, 10], Json2, Schema).
ejsondb:get(b, [2, 10], Json3, Schema).
%> {error, not_found}

%% SET_ALL/GET_ALL
Json4 = ejsondb:set_all(b, [2], [], Json3, Schema).
ejsondb:get_all(b, [2], Json4, Schema).
%> []
```