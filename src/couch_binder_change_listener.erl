%%%-------------------------------------------------------------------
%%% @author ahmetturk
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. Feb 2018 09:44
%%%-------------------------------------------------------------------
-module(couch_binder_change_listener).
-author("ahmetturk").
-behavior(gen_server).

-include("couch/include/couch_db.hrl").
-include("couch_binder.hrl").


%% API
-export([init/1, handle_call/3, handle_cast/2, start/1, add_listener/2, remove_listener/2, stop/1, ensure_db_exists/2, ensure_db_exists/1, initialize/1]).

-record(state, {
  loop,
  listeners = [],
  db_name,
  changes_feed_fun
}).


init(DbName) ->
  process_flag(trap_exit, true),
  {ok, #state{loop = changes_feed_loop(DbName), db_name = DbName, listeners = []}}.


changes_feed_loop(DbName) ->
  {ok, ClosedDb} = ensure_db_exists(DbName),
  Server = self(),
  Pid = spawn_link(
    fun() ->
      DbOpenOptions = [{user_ctx, ClosedDb#db.user_ctx}, sys_db],
      {ok, Db} = couch_db:open_int(DbName, DbOpenOptions),
      ChangesFeedFun = couch_changes:handle_changes(
        #changes_args{
          include_docs = true,
          feed = "continuous",
          timeout = infinity
        },
        {json_req, null},
        Db
      ),
      gen_server:call(Server, {changes_feed, ChangesFeedFun}, infinity)
    end
  ),
  Pid.


handle_call({add_listener, #change_listener{name = Name} = Listener}, _From, #state{listeners = Listeners} = State) ->
  Prop = proplists:property(Name, Listener),
  CleanListeners =
    case proplists:is_defined(Name, Listeners) of
      true -> proplists:delete(Name, Listeners);
      _ -> Listeners
    end,
  {reply, ok, State#state{listeners = CleanListeners ++ [Prop]}};
handle_call({remove_listener, Name}, _From, #state{listeners = Listeners} = State) ->
  {reply, ok, State#state{listeners = proplists:delete(Name, Listeners)}};
handle_call({db_changed, Change}, _From, #state{listeners = Listeners} = State) ->
  broadcast_change(Change, Listeners),
  {reply, ok, State};
handle_call({changes_feed, ChangesFeedFun}, _From, State) ->
  {reply, ok, State#state{changes_feed_fun = ChangesFeedFun}};
handle_call(start_listen, _From, #state{changes_feed_fun = ChangesFeedFun} = State) ->
  Server = self(),
  ChangesFeedFun(
    fun({change, Change, _}, _) ->
      gen_server:call(Server, {db_changed, Change}, infinity);
      (_, _) ->
        ok
    end
  ),
  {reply, ok, State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.


handle_cast(_Request, State) ->
  {noreply, State}.

ensure_db_exists(DbName) ->
  ensure_db_exists(DbName, true).
ensure_db_exists(DbName, Close) ->
  UserCtx = #user_ctx{roles = [<<"_admin">>]},
  Db =
    case couch_db:open_int(DbName, [sys_db, {user_ctx, UserCtx}, nologifmissing]) of
      {ok, Db} ->
        Db;
      _Error ->
        {ok, Db} = couch_db:create(DbName, [sys_db, {user_ctx, UserCtx}]),
        Db
    end,
  case Close of
    true -> couch_db:close(Db);
    _ -> ok
  end,
  {ok, Db}.


broadcast_change(_Change, []) ->
  ok;
broadcast_change(Change, [#change_listener{filter = Filter, func = Func} | Listeners]) ->
  case is_function(Filter) of
    true ->
      case Filter(Change) of
        true -> Func(Change);
        _ -> ok
      end;
    _ -> Func(Change)
  end,
  broadcast_change(Change, Listeners).

initialize(DbName) ->
  ?LOG_INFO("initialize(\"~w\") called", [DbName]),
  ChildId = to_id(DbName),
  ChildSpec = {
    ChildId,
    {gen_server, start_link, [?MODULE, DbName, []]},
    temporary,
    250,
    worker,
    [?MODULE]
  },

  case supervisor:start_child(couch_binder_change_listener_sup, ChildSpec) of
    {ok, Pid} ->
      {ok, Pid};
    {error, already_present} ->
      case supervisor:restart_child(couch_binder_change_listener_sup, ChildId) of
        {ok, Pid} ->
          {ok, Pid};
        {error, running} ->
          {error, {already_started, Pid}} = supervisor:start_child(couch_binder_change_listener_sup, ChildSpec),
          {ok, Pid};
        {error, {'EXIT', {badarg, [{erlang, apply, [gen_server, start_link, undefined]} | _]}}} ->
          _ = supervisor:delete_child(couch_binder_change_listener_sup, ChildId),
          initialize(DbName);
        {error, _} = Error ->
          Error
      end;
    {error, {already_started, Pid}} ->
      {ok, Pid};
    {error, {Error, _}} ->
      {error, Error}
  end,
  ?LOG_INFO("initialize(\"~w\") finished", [DbName]).

start(DbName) ->
  case find_child(DbName) of
    {ok, Pid} ->
      gen_server:call(Pid, start_listen, infinity);
    {error, not_found} ->
      initialize(DbName),
      start(DbName)
  end.

stop(DbName) ->
  ChildId = to_id(DbName),
  supervisor:terminate_child(couch_binder_change_listener_sup, ChildId).


add_listener(DbName, Listener) ->
  case find_child(DbName) of
    {ok, Pid} ->
      gen_server:call(Pid, {add_listener, Listener}, infinity);
    {error, not_found} ->
      initialize(DbName),
      add_listener(DbName, Listener)
  end.


remove_listener(DbName, Name) ->
  case find_child(DbName) of
    {ok, Pid} ->
      gen_server:call(Pid, {remove_listener, Name}, infinity);
    {error, not_found} ->
      initialize(DbName),
      remove_listener(DbName, Name)
  end.

to_id(DbName) when is_list(DbName) ->
  to_id(list_to_binary(DbName));
to_id(DbName) ->
  couch_util:to_hex(couch_util:md5(DbName)).


find_child(DbName) ->
  ChildId = to_id(DbName),
  Response = case lists:keysearch(
    ChildId, 1, supervisor:which_children(couch_binder_change_listener_sup)) of
               {value, {_, Pid, _, _}} when is_pid(Pid) ->
                 {ok, Pid};
               _ ->
                 {error, not_found}
             end,
  Response.