-module(couch_binder_job_sup).
-behaviour(supervisor).
-export([init/1, start_link/0]).

-include("couch/include/couch_db.hrl").
-include("couch_binder.hrl").
start_link() ->
  ?LOG_INFO("Change listener supervisor initializing", []),
  ChangeListenerSpec = {{one_for_one, 10, 3600},
    [
      {couch_binder_change_listener,
        {couch_binder_change_listener_sup, start_link, []},
        permanent,
        infinity,
        supervisor,
        [couch_binder_change_listener_sup]}
    ]},
  supervisor:start_link({local, ?MODULE}, ?MODULE, ChangeListenerSpec).

%%=============================================================================
%% supervisor callbacks
%%=============================================================================

init(ChangeListenerSpec) ->
  {ok, ChangeListenerSpec}.

%%=============================================================================
%% internal functions
%%=============================================================================
