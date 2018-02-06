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
  changes_feed_loop = nil,
  db_notifier = nil,
  binding_db_name = nil,
  binding_start_pids = []
}).

-define(DOC_TO_BINDING, couch_binder_doc_to_binder).
-define(BINDING_TO_STATE, couch_binder_binder_to_state).

%% API
-export([init/1, start_link/0, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
  process_flag(trap_exit, true),
  {Loop, BinderDbName} = changes_feed_loop(),
  {ok, [{loop, Loop}, {db, BinderDbName}]}.

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
          case has_valid_binder_id(Change) of
            true ->
              ok = gen_server:call(Server, {db_changed, Change}, infinity);
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

ensure_binder_db_exists() ->
  DbName = ?l2b(couch_config:get("binder", "db", "binders")),
  UserCtx = #user_ctx{roles = [<<"_admin">>]},
  case couch_db:open_int(DbName, [sys_db, {user_ctx, UserCtx}, nologifmissing]) of
    {ok, Db} ->
      Db;
    _Error ->
      {ok, Db} = couch_db:create(DbName, [sys_db, {user_ctx, UserCtx}])
  end,
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
      ok;
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
    % Shouldn't happen, as by default only the role binding can
    % update binding documents.
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
      RepState = #binding_state{
        binding = Binding,
        starting = true
      },
      true = ets:insert(?BINDING_TO_STATE, {BindingId, RepState}),
      true = ets:insert(?DOC_TO_BINDING, {DocId, BindingId}),
      ?LOG_INFO("Attempting to start binding `~s` (document `~s`).",
        [pp_binding_id(BindingId), DocId]),
      Pid = spawn_link(fun() -> start_binding(Binding, 0) end),
      State#state{binding_start_pids = [Pid | State#state.binding_start_pids]};
    #binding_state{binding = #binding{doc_id = DocId}} ->
      State;
    #binding_state{starting = false, binding = #binding{doc_id = OtherDocId}} ->
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
  erlang:error(not_implemented).

start_binding(Binding, N) ->
  erlang:error(not_implemented).

maybe_tag_binding_doc(DocId, BindingDoc, L2b) ->
  erlang:error(not_implemented).

has_valid_binder_id({Change}) ->
  has_valid_binder_id(get_value(<<"id">>, Change));
has_valid_binder_id(<<?DESIGN_DOC_PREFIX, _Rest/binary>>) ->
  false;
has_valid_binder_id(_Else) ->
  true.