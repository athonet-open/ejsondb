-ifndef(_ejsondb_h_).
-define(_ejsondb_h_, true).

-type ejsondb_query_entry() :: {Qid :: atom(), Jsonpath :: string(), QArity :: number()}.
-type ejsondb_id_key() :: binary() | atom().

-record(ejsondb_schema, {
  qtab = [] :: list(ejsondb_query_entry()),
  id_key :: ejsondb_id_key(),
  funs = #{} :: ejsonpath:jsonpath_funcspecs(),
  opts = []  :: list()}).
-endif.