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
-include("couch_binder.hrl").

-behaviour(gen_server).

-import(couch_util, [
get_value/2,
get_value/3,
to_binary/1
]).

-define(INITIAL_WAIT, 2.5). % seconds

-record(binding_state, {
  binding,
  starting,
  wait = ?INITIAL_WAIT
}).

-record(state, {
  changes_listener = nil,
  binder_db_name = nil,
  binding_start_pids = []
}).

-define(DOC_TO_BINDING, couch_binder_doc_to_binder).
-define(BINDING_TO_STATE, couch_binder_binder_to_state).

%% API
-export([init/1, start_link/0, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3, has_valid_binder_id/1]).

start_link() ->
  initialize_supervisors(),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
  process_flag(trap_exit, true),
  {Listener, BinderDbName} = changes_feed_loop(),
  {ok, #state{
    changes_listener = Listener,
    binder_db_name = BinderDbName
  }}.

changes_feed_loop() ->
  ?LOG_INFO("changes_feed_loop", []),
  {ok, BinderDb} = ensure_binder_db_exists(),
  BinderDbName = couch_db:name(BinderDb),
  couch_db:close(BinderDb),
  Server = self(),
  Listener = #change_listener{
    name = BinderDbName,

    filter = fun has_valid_binder_id/1,
    func =
    fun(Change) ->
      ok = gen_server:call(Server, {db_changed, Change}, infinity)
    end
  },
  spawn_link(
    fun() ->
      ?LOG_INFO("before couch_binder_change_listener:add_listener(~w, ~w)", [BinderDbName, Listener]),
      couch_binder_change_listener:add_listener(BinderDbName, Listener),
      ?LOG_INFO("after couch_binder_change_listener:add_listener(BinderDbName, Listener)", []),
      ?LOG_INFO("before couch_binder_change_listener:start(~w)", [BinderDbName]),
      couch_binder_change_listener:start(BinderDbName),
      ?LOG_INFO("after couch_binder_change_listener:start(BinderDbName)", [])
    end
  ),
  ?LOG_INFO("return changes_feed_loop", []),
  {Listener, BinderDbName}.

ensure_binder_db_exists() ->
  DbName = ?l2b(couch_config:get("binder", "db", "binders")),
  {ok, Db} = couch_binder_change_listener:ensure_db_exists(DbName, false),
  ensure_binder_ddoc_exists(Db, <<"_design/binders">>),
  {ok, Db}.

ensure_binder_ddoc_exists(BinderDb, DDocID) ->
  case couch_db:open_doc(BinderDb, DDocID, []) of
    {ok, _Doc} ->
      ok;
    _ ->
      DDoc = couch_doc:from_json_obj({[
        {<<"_id">>, DDocID},
        {<<"language">>, <<"javascript">>},
        {<<"validate_doc_update">>, <<"function(newDoc, oldDoc, userCtx) {}">>}
      ]}),
      {ok, _Rev} = couch_db:update_doc(BinderDb, DDoc, [])
  end.

handle_call({db_changed, {ChangeProps} = Change}, _From, State) ->
  NewState = try
               on_changes(State, Change)
             catch
               _Tag:Error ->
                 {BindingProps} = get_value(doc, ChangeProps),
                 DocId = get_value(<<"_id">>, BindingProps),
                 binder_db_update_error(Error, DocId),
                 State
             end,
  {reply, ok, NewState}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  gen_server:cast(?MODULE, stop).

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

on_changes(State, {Change}) ->
  {BindingProps} = JsonBindingDoc = get_value(doc, Change),
  DocId = get_value(<<"_id">>, BindingProps),
  case get_value(<<"deleted">>, Change, false) of
    true ->
      binder_doc_deleted(DocId),
      State;
    false ->
      case get_value(<<"binding_state">>, BindingProps) of
        undefined ->
          maybe_start_binding(State, DocId, JsonBindingDoc);
        <<"triggered">> ->
          maybe_start_binding(State, DocId, JsonBindingDoc);
        <<"error">> ->
          case ets:lookup(?DOC_TO_BINDING, DocId) of
            [] ->
              maybe_start_binding(State, DocId, JsonBindingDoc);
            _ ->
              State
          end
      end
  end.

binder_db_update_error(Error, DocId) ->
  case Error of
    {bad_binding_doc, Reason} ->
      Reason = Reason;
    _ ->
      Reason = to_binary(Error)
  end,
  ?LOG_ERROR("Binder manager, error processing document `~s`: ~s", [DocId, Reason]),
  update_binder_doc(DocId, [{<<"binding_state">>, <<"error">>}, {<<"binding_state_reason">>, Reason}]).


update_binder_doc(DocId, KVs) ->
  {ok, BindingDb} = ensure_binder_db_exists(),
  try
    case couch_db:open_doc(BindingDb, DocId, [ejson_body]) of
      {ok, LatestBindingDoc} ->
        update_binder_doc(BindingDb, LatestBindingDoc, KVs);
      _ ->
        ok
    end
  catch throw:conflict ->
    ?LOG_ERROR("Conflict error when updating binding document `~s`. Retrying.", [DocId]),
    ok = timer:sleep(5),
    update_binder_doc(DocId, KVs)
  after
    couch_db:close(BindingDb)
  end.

update_binder_doc(BindingDb, #doc{body = {BindingDocBody}} = BindingDoc, KVs) ->
  NewBindingDocBody = lists:foldl(
    fun({K, undefined}, Body) ->
      lists:keydelete(K, 1, Body);
      ({<<"binding_state">> = K, State} = KV, Body) ->
        case get_value(K, Body) of
          State ->
            Body;
          _ ->
            Body1 = lists:keystore(K, 1, Body, KV),
            lists:keystore(
              <<"binding_state_time">>, 1, Body1,
              {<<"binding_state_time">>, timestamp()})
        end;
      ({K, _V} = KV, Body) ->
        lists:keystore(K, 1, Body, KV)
    end,
    BindingDocBody, KVs),
  case NewBindingDocBody of
    BindingDocBody ->
      ok;
    _ ->
      % Might not succeed - when the binding doc is deleted right
      % before this update (not an error, ignore).
      couch_db:update_doc(BindingDb, BindingDoc#doc{body = {NewBindingDocBody}}, [])
  end.

binder_doc_deleted(DocId) ->
  case ets:lookup(?DOC_TO_BINDING, DocId) of
    [{DocId, BindingId}] ->
      couch_binder:cancel_binding(BindingId),
      true = ets:delete(?BINDING_TO_STATE, BindingId),
      true = ets:delete(?DOC_TO_BINDING, DocId),
      ?LOG_INFO("Stopped binding `~s` because binding document `~s` was deleted", [pp_binding_id(BindingId), DocId]);
    [] ->
      ok
  end.

pp_binding_id(#binding{id = BindingId}) ->
  pp_binding_id(BindingId);
pp_binding_id({Base, Extension}) ->
  Base ++ Extension.


maybe_start_binding(State, DocId, {BindingDoc}) ->
  Binding = parse_binding_doc(BindingDoc),
  #binding{id = {BaseId, _} = BindingId} = Binding,
  case binding_state(BindingId) of
    nil ->
      BindingState = #binding_state{
        binding = Binding,
        starting = true
      },
      true = ets:insert(?BINDING_TO_STATE, {BindingId, BindingState}),
      true = ets:insert(?DOC_TO_BINDING, {DocId, BindingId}),
      ?LOG_INFO("Attempting to start binding `~s` (document `~s`).", [pp_binding_id(BindingId), DocId]),
      Pid = spawn_link(fun() -> start_binding(Binding, 0) end),
      State#state{binding_start_pids = [Pid | State#state.binding_start_pids]};
    #binding_state{binding = #binding{doc_id = DocId}} ->
      State;
    #binding_state{starting = false, binding = #binding{doc_id = OtherDocId}} -> % todo id must generate by document content for conflict
      ?LOG_INFO("The binding specified by the document `~s` was already triggered by the document `~s`", [DocId, OtherDocId]),
      maybe_tag_binding_doc(DocId, BindingDoc, ?l2b(BaseId)),
      State;
    #binding_state{starting = true, binding = #binding{doc_id = OtherDocId}} ->
      ?LOG_INFO("The binding specified by the document `~s` is already being triggered by the document `~s`", [DocId, OtherDocId]),
      maybe_tag_binding_doc(DocId, BindingDoc, ?l2b(BaseId)),
      State
  end.


parse_binding_doc(BindingDoc) ->
  {ok, Binding} =
    try
      {ok,
        #binding{
          db_one = get_value(<<"db_one">>, BindingDoc),
          db_many = get_value(<<"db_many">>, BindingDoc),
          field_one = get_value(<<"field_one">>, BindingDoc),
          doc_id = get_value(<<"_id">>, BindingDoc),
          id = {couch_util:to_hex(couch_util:md5(couch_server:get_uuid())), couch_util:to_hex(couch_util:md5(couch_server:get_uuid()))}
        }
      }
    catch
      throw:{error, Reason} ->
        throw({bad_binding_doc, Reason});
      Tag:Err ->
        throw({bad_binding_doc, to_binary({Tag, Err})})
    end,
  Binding.


timestamp() ->
  {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:now_to_local_time(now()),
  UTime = erlang:universaltime(),
  LocalTime = calendar:universal_time_to_local_time(UTime),
  DiffSecs = calendar:datetime_to_gregorian_seconds(LocalTime) -
    calendar:datetime_to_gregorian_seconds(UTime),
  zone(DiffSecs div 3600, (DiffSecs rem 3600) div 60),
  iolist_to_binary(
    io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w~s",
      [Year, Month, Day, Hour, Min, Sec,
        zone(DiffSecs div 3600, (DiffSecs rem 3600) div 60)])).

zone(Hr, Min) when Hr >= 0, Min >= 0 ->
  io_lib:format("+~2..0w:~2..0w", [Hr, Min]);
zone(Hr, Min) ->
  io_lib:format("-~2..0w:~2..0w", [abs(Hr), abs(Min)]).



binding_state(BindingId) ->
  case ets:lookup(?BINDING_TO_STATE, BindingId) of
    [{BindingId, BindingState}] ->
      BindingState;
    [] ->
      nil
  end.


start_binding(Binding, Wait) ->
  ok = timer:sleep(Wait * 1000),
  case (catch couch_binder:async_binding(Binding)) of
    {ok, _} ->
      ok;
    Error ->
      binding_error(Binding, Error)
  end.

maybe_tag_binding_doc(DocId, {BindingProps}, BindingId) ->
  case get_value(<<"binding_id">>, BindingProps) of
    BindingId -> ok;
    _ ->
      update_binding_doc(DocId, [{<<"binding_id">>, BindingId}])
  end.


has_valid_binder_id({Change}) ->
  has_valid_binder_id(get_value(<<"id">>, Change));
has_valid_binder_id(<<?DESIGN_DOC_PREFIX, _Rest/binary>>) ->
  false;
has_valid_binder_id(_Else) ->
  true.



binding_error(#binding{id = {BaseId, _} = BindingId}, Error) ->
  case binding_state(BindingId) of
    nil ->
      ok;
    #binding_state{binding = #binding{doc_id = DocId}} ->
      update_binding_doc(DocId, [
        {<<"binding_state">>, <<"error">>},
        {<<"binding_state_reason">>, to_binary(error_reason(Error))},
        {<<"binding_id">>, ?l2b(BaseId)}]),
      ok = gen_server:call(?MODULE, {binding_error, BindingId, Error}, infinity)
  end.


error_reason({error, {Error, Reason}})
  when is_atom(Error), is_binary(Reason) ->
  io_lib:format("~s: ~s", [Error, Reason]);
error_reason({error, Reason}) ->
  Reason;
error_reason(Reason) ->
  Reason.

update_binding_doc(BindingDocId, KVs) ->
  {ok, BindingDb} = ensure_binder_db_exists(),
  try
    case couch_db:open_doc(BindingDb, BindingDocId, [ejson_body]) of
      {ok, LatestBindingDoc} ->
        update_binding_doc(BindingDb, LatestBindingDoc, KVs);
      _ ->
        ok
    end
  catch throw:conflict ->
    ?LOG_ERROR("Conflict error when updating binding document `~s`. Retrying.", [BindingDocId]),
    ok = timer:sleep(5),
    update_binding_doc(BindingDocId, KVs)
  after
    couch_db:close(BindingDb)
  end.


update_binding_doc(BindingDb, #doc{body = {BindingDocBody}} = BindingDoc, KVs) ->
  NewBindingDocBody = lists:foldl(
    fun({K, undefined}, Body) ->
      lists:keydelete(K, 1, Body);
      ({<<"binding_state">> = K, State} = KV, Body) ->
        case get_value(K, Body) of
          State ->
            Body;
          _ ->
            Body1 = lists:keystore(K, 1, Body, KV),
            lists:keystore(
              <<"binding_state_time">>, 1, Body1,
              {<<"binding_state_time">>, timestamp()})
        end;
      ({K, _V} = KV, Body) ->
        lists:keystore(K, 1, Body, KV)
    end,
    BindingDocBody, KVs),
  case NewBindingDocBody of
    BindingDocBody ->
      ok;
    _ ->
      couch_db:update_doc(BindingDb, BindingDoc#doc{body = {NewBindingDocBody}}, [])
  end.



initialize_supervisors() ->
  BinderJobSpec = {
    couch_binder_job_sup,
    {couch_binder_job_sup, start_link, []},
    permanent,
    infinity,
    supervisor,
    [couch_binder_job_sup]
  },
  ?LOG_INFO("Binder Job supervisor initializing", []),
  couch_binder_job_sup:start_link().