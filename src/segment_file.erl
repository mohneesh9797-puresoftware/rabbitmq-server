%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(segment_file).

-export([parse_segment_entries/2]).

%% TODO: The segment size would be configurable, but deriving all the
%% other values is quite hairy and quite possibly noticeably less
%% efficient, depending on how clever the compiler is when it comes to
%% binary generation/matching with constant vs variable lengths.

-define(REL_SEQ_BITS, 14).
%% calculated as trunc(math:pow(2,?REL_SEQ_BITS))).
-define(SEGMENT_ENTRY_COUNT, 16384).

%% seq only is binary 01 followed by 14 bits of rel seq id
%% (range: 0 - 16383)
-define(REL_SEQ_ONLY_PREFIX, 01).
-define(REL_SEQ_ONLY_PREFIX_BITS, 2).
-define(REL_SEQ_ONLY_RECORD_BYTES, 2).

%% publish record is binary 1 followed by a bit for is_persistent,
%% then 14 bits of rel seq id, 64 bits for message expiry, 32 bits of
%% size and then 128 bits of md5sum msg id.
-define(PUB_PREFIX, 1).
-define(PUB_PREFIX_BITS, 1).

-define(EXPIRY_BYTES, 8).
-define(EXPIRY_BITS, (?EXPIRY_BYTES * 8)).
-define(NO_EXPIRY, 0).

-define(MSG_ID_BYTES, 16). %% md5sum is 128 bit or 16 bytes
-define(MSG_ID_BITS, (?MSG_ID_BYTES * 8)).

%% This is the size of the message body content, for stats
-define(SIZE_BYTES, 4).
-define(SIZE_BITS, (?SIZE_BYTES * 8)).

%% This is the size of the message record embedded in the queue
%% index. If 0, the message can be found in the message store.
-define(EMBEDDED_SIZE_BYTES, 4).
-define(EMBEDDED_SIZE_BITS, (?EMBEDDED_SIZE_BYTES * 8)).

%% 16 bytes for md5sum + 8 for expiry
-define(PUB_RECORD_BODY_BYTES, (?MSG_ID_BYTES + ?EXPIRY_BYTES + ?SIZE_BYTES)).
%% + 4 for size
-define(PUB_RECORD_SIZE_BYTES, (?PUB_RECORD_BODY_BYTES + ?EMBEDDED_SIZE_BYTES)).

%% + 2 for seq, bits and prefix
-define(PUB_RECORD_PREFIX_BYTES, 2).

-export_type([segment_entry/0]).

-type segment_entry() :: {{IsPersistent :: boolean(),
                           Bin :: binary(),
                           MsgBin :: binary()},
                          IsDelivered :: atom(),
                          IsAcked :: atom()}.

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
