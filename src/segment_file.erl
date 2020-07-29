%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(segment_file).

-include("rabbit_queue_index.hrl").

-export([parse_segment_entries/2]).

-export_type([segment_entry/0]).

-type segment_entry() :: {pub(),
                          IsDelivered :: 'del' | 'no_del',
                          IsAcked :: 'ack' | 'no_ack'}.

%% Entries are indexed by their RelSeq
-spec parse_segment_entries(binary(), boolean()) ->
          {array:array(segment_entry()), UnackedCount :: integer()}.
parse_segment_entries(SegBin, KeepAcked) ->
    Empty = {array:new([{default, undefined}, fixed, {size, ?SEGMENT_ENTRY_COUNT}]),
             0},
    parse_segment_entries(SegBin, KeepAcked, Empty).

%% Private

parse_segment_entries(<<?PUB_PREFIX:?PUB_PREFIX_BITS,
  IsPersistNum:1, RelSeq:?REL_SEQ_BITS, Rest/binary>>,
    KeepAcked, Acc) ->
  parse_segment_publish_entry(
    Rest, 1 == IsPersistNum, RelSeq, KeepAcked, Acc);
parse_segment_entries(<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
  RelSeq:?REL_SEQ_BITS, Rest/binary>>, KeepAcked, Acc) ->
  parse_segment_entries(
    Rest, KeepAcked, add_segment_relseq_entry(KeepAcked, RelSeq, Acc));
parse_segment_entries(<<>>, _KeepAcked, Acc) ->
  Acc.

parse_segment_publish_entry(<<Bin:?PUB_RECORD_BODY_BYTES/binary,
  MsgSize:?EMBEDDED_SIZE_BITS,
  MsgBin:MsgSize/binary, Rest/binary>>,
    IsPersistent, RelSeq, KeepAcked,
    {SegEntries, Unacked}) ->
  Obj = {{IsPersistent, Bin, MsgBin}, no_del, no_ack},
  SegEntries1 = array:set(RelSeq, Obj, SegEntries),
  parse_segment_entries(Rest, KeepAcked, {SegEntries1, Unacked + 1});
parse_segment_publish_entry(Rest, _IsPersistent, _RelSeq, KeepAcked, Acc) ->
  parse_segment_entries(Rest, KeepAcked, Acc).

add_segment_relseq_entry(KeepAcked, RelSeq, {SegEntries, Unacked}) ->
  case array:get(RelSeq, SegEntries) of
    {Pub, no_del, no_ack} ->
      {array:set(RelSeq, {Pub, del, no_ack}, SegEntries), Unacked};
    {Pub, del, no_ack} when KeepAcked ->
      {array:set(RelSeq, {Pub, del, ack},    SegEntries), Unacked - 1};
    {_Pub, del, no_ack} ->
      {array:reset(RelSeq,                   SegEntries), Unacked - 1}
  end.
