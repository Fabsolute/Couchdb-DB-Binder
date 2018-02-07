%%%-------------------------------------------------------------------
%%% @author fabsolutely
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 05. Feb 2018 23:24
%%%-------------------------------------------------------------------
-module(couch_binder).
-author("fabsolutely").

-include("couch/include/couch_db.hrl").
-include("couch_binder.hrl").

%% API
-export([cancel_binding/1, async_binding/1, init/1, handle_call/3, handle_cast/2]).

-behavior(gen_server).

-import(couch_util, [
get_value/2,
get_value/3,
to_binary/1
]).

-record(state, {
  binding
}).

init(#binding{db_one = DbOne, db_many = DbMany} = Binding) ->
  Server = self(),
  OneListener = #change_listener{
    name = binder_one_listener,
    filter = fun couch_binder_manager:has_valid_binder_id/1,
    func =
    fun(Change) ->
      gen_server:call(Server, {one_changed, Change}, infinity)
    end
  },
  ManyListener = #change_listener{
    name = binder_many_listener,
    filter = fun couch_binder_manager:has_valid_binder_id/1,
    func =
    fun(Change) ->
      gen_server:call(Server, {many_changed, Change}, infinity)
    end
  },

  couch_binder_change_listener:add_listener(DbOne, OneListener),
  couch_binder_change_listener:add_listener(DbMany, ManyListener),

  couch_binder_change_listener:start(DbOne),
  couch_binder_change_listener:start(DbMany),

  {ok, #state{binding = Binding}}.

handle_call({one_changed, Change}, _From, State) ->
  ?LOG_INFO("one db changed ~n~w~n", [Change]),
  {reply, ok, State};
handle_call({many_changed, Change}, _From, State) ->
  ?LOG_INFO("many db changed ~n~w~n", [Change]),
  {reply, ok, State}.

handle_cast(_Request, State) ->
  {noreply, State}.


cancel_binding({BaseId, Extension}) ->
  FullBindingId = BaseId ++ Extension,
  ?LOG_INFO("Canceling binding `~s`...", [FullBindingId]),
  case supervisor:terminate_child(couch_binder_job_sup, FullBindingId) of
    ok ->
      ?LOG_INFO("Binding `~s` canceled.", [FullBindingId]),
      case supervisor:delete_child(couch_binder_job_sup, FullBindingId) of
        ok ->
          {ok, {cancelled, ?l2b(FullBindingId)}};
        {error, not_found} ->
          {ok, {cancelled, ?l2b(FullBindingId)}};
        Error ->
          Error
      end;
    Error ->
      ?LOG_ERROR("Error canceling binding `~s`: ~p", [FullBindingId, Error]),
      Error
  end.

async_binding(#binding{id = {BaseId, Ext}, db_one = DbOne, db_many = DbMany} = Binding) ->
  BindingChildId = BaseId ++ Ext,
  ChildSpec = {
    BindingChildId,
    {gen_server, start_link, [?MODULE, Binding, []]},
    temporary,
    250,
    worker,
    [?MODULE]
  },

  case supervisor:start_child(couch_binder_job_sup, ChildSpec) of
    {ok, Pid} ->
      ?LOG_INFO("starting new binding `~s` at ~p (`~s` -> `~s`)",
        [BindingChildId, Pid, DbOne, DbMany]),
      {ok, Pid};
    {error, already_present} ->
      case supervisor:restart_child(couch_binder_job_sup, BindingChildId) of
        {ok, Pid} ->
          ?LOG_INFO("restarting binding `~s` at ~p (`~s` -> `~s`)",
            [BindingChildId, Pid, DbOne, DbMany]),
          {ok, Pid};
        {error, running} ->
          {error, {already_started, Pid}} = supervisor:start_child(couch_binder_job_sup, ChildSpec),
          ?LOG_INFO("binding `~s` already running at ~p (`~s` -> `~s`)",
            [BindingChildId, Pid, DbOne, DbMany]),
          {ok, Pid};
        {error, {'EXIT', {badarg,
          [{erlang, apply, [gen_server, start_link, undefined]} | _]}}} ->
          _ = supervisor:delete_child(couch_binder_job_sup, BindingChildId),
          async_binding(Binding);
        {error, _} = Error ->
          Error
      end;
    {error, {already_started, Pid}} ->
      ?LOG_INFO("binding `~s` already running at ~p (`~s` -> `~s`)",
        [BindingChildId, Pid, DbOne, DbMany]),
      {ok, Pid};
    {error, {Error, _}} ->
      {error, Error}
  end.

find_child(#binding{id = {BaseId, Ext}}) ->
  BindingChildId = BaseId ++ Ext,
  case lists:keysearch(
    BindingChildId, 1, supervisor:which_children(couch_binder_job_sup)) of
    {value, {_, Pid, _, _}} when is_pid(Pid) ->
      {ok, Pid};
    _ ->
      {error, not_found}
  end.