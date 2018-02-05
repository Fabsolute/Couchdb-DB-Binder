%%%-------------------------------------------------------------------
%%% @author ahmetturk
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 05. Feb 2018 16:13
%%%-------------------------------------------------------------------
-module(couch_binder_manager).
-author("ahmetturk").
-include("couch/include/couch_db.hrl").

%% API
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-behaviour(gen_server).
-define(DOC_TO_BIND, couch_bind_doc_id_to_bind_id).
-define(BIND_TO_STATE, couch_bind_id_to_bind_state).

-record(state, {
  changes_feed_loop = nil,
  db_notifier = nil,
  binder_db_name = nil,
  binder_start_pids = [],
  max_retries
}).

init(_) ->
  process_flag(trap_exit, true),
  ?DOC_TO_BIND = ets:new(?DOC_TO_BIND, [named_table, set, protected]),
  ?BIND_TO_STATE = ets:new(?BIND_TO_STATE, [named_table, set, protected]),
  Server = self(),
  ok = couch_config:register(
    fun("binder", "db", NewDBName) ->
      ok = gen_server:cast(Server, {binder_db_changed, list_to_binary(NewDBName)})
    end
  ),
  {Loop, BinderDbName} = changes_feed_loop(),
  {ok, #state{
    changes_feed_loop = Loop,
    binder_db_name = BinderDbName,
    db_notifier = db_update_notifier(),
    max_retries = retries_value(couch_config:get("binder", "max_replication_retry_count", "10"))
  }}.
.

changes_feed_loop() ->
  {ok, BinderDb} = ensure_binder_db_exists(),
  BinderDbName = couch_db:name(BinderDb),
  couch_db:close(BinderDb),
  Server = self(),
  Pid = spawn_link(
    fun() ->
      DbOpenOptions = [{user_ctx, BinderDb#db.user_ctx}, sys_db],
      {ok, Db} = couch_db:open_int(BinderDbName, DbOpenOptions),
      ChangesFeedFun = couch_changes:handle_changes(
        #changes_args{
          include_docs = true,
          feed = "continuous",
          timeout = infinity
        },
        {json_req, null},
        Db
      ),
      ChangesFeedFun(
        fun({change, Change, _}, _) ->
          case has_valid_rep_id(Change) of
            true ->
              ok = gen_server:call(
                Server, {rep_db_update, Change}, infinity);
            false ->
              ok
          end;
          (_, _) ->
            ok
        end
      )
    end
  ),
  {Pid, BinderDbName}.

db_update_notifier() ->
  todo.

retries_value(CouchConfigget) ->
  todo.

ensure_binder_db_exists() ->
  todo.

has_valid_rep_id(Change) ->
  todo.

handle_cast({binder_db_changed, NewDBName}, State) ->
  todo;

handle_cast(Request, State) ->
  erlang:error(not_implemented).

handle_call(Request, From, State) ->
  erlang:error(not_implemented).

handle_info(Info, State) ->
  erlang:error(not_implemented).

terminate(Reason, State) ->
  erlang:error(not_implemented).

code_change(OldVsn, State, Extra) ->
  erlang:error(not_implemented).




