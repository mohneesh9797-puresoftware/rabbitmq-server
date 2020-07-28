%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_seq2_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          ofSparseArrayReversed
        ]}
    ].

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

ofSparseArrayReversed(_Config) ->
    SparseArray1 = array:new([{default, undefined}, fixed, {size, 6}]),
    SparseArray2 = array:set(0, "a", SparseArray1),
    SparseArray3 = array:set(3, "b", SparseArray2),
    SparseArray4 = array:set(5, "c", SparseArray3),
    ?assertEqual([{5, "c"}, {3, "b"}, {0, "a"}],
                 seq:toList(seq2:ofSparseArrayReversed(SparseArray4))).
