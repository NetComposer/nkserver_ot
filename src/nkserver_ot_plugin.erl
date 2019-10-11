%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Carlos Gonzalez Florido.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Default callbacks for plugin definitions
-module(nkserver_ot_plugin).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([plugin_config/3, plugin_cache/3, plugin_start/3, plugin_update/4]).



%% ===================================================================
%% Default implementation
%% ===================================================================


plugin_config(_Id, Config, _Service) ->
    Syntax = #{
        % See nkserver_ot_rules
        opentrace_prefix => binary,
        opentrace_filter => binary
    },
    nkserver_util:parse_config(Config, Syntax).


plugin_cache(Id, Config, _Service) ->
    {ok, #{
        activated => nkserver_ot_app:get(activate),
        prefix => maps:get(opentrace_prefix, Config, <<>>),
        rules_mod => nkserver_ot_rules:get_rules_mod(Id)
    }}.


plugin_start(Id, Config, _Service) ->
    update_trace_filter(Id, Config),
    ok.


plugin_update(Id, NewConfig, _OldConfig, _Service) ->
    lager:notice("Recompiling OT rules for ~s", [Id]),
    update_trace_filter(Id, NewConfig),
    ok.



%% ===================================================================
%% Internal
%% ===================================================================




update_trace_filter(SrvId, Config) ->
    case maps:find(opentrace_filter, Config) of
        {ok, Filter} ->
            ok = nkserver_ot_rules:compile(SrvId, Filter);
        error ->
            nkserver_ot_rules:clear(SrvId)
    end.

