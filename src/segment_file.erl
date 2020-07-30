%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(segment_file).

-include("rabbit.hrl").
-include("rabbit_queue_index.hrl").

-export([parse_segment_entries/2,
         parse_pub_record_body/2,
         store_msg_segment/1,
         store_msg_size_segment/1,
         add_queue_ttl_segment/1,
         avoid_zeroes_segment/1]).

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

% This mirrors the encoding performed by journal_file:encode_pub_record_body/2
-spec parse_pub_record_body(Bin :: binary(), MsgBin :: binary()) ->
          {rabbit_types:msg_id(), rabbit_types:message_properties()}.
parse_pub_record_body(<<MsgIdNum:?MSG_ID_BITS, Expiry:?EXPIRY_BITS,
                        Size:?SIZE_BITS>>, MsgBin) ->
    %% work around for binary data fragmentation. See
    %% rabbit_msg_file:read_next/2
    <<MsgId:?MSG_ID_BYTES/binary>> = <<MsgIdNum:?MSG_ID_BITS>>,
    Props = #message_properties{expiry = case Expiry of
                                             ?NO_EXPIRY -> undefined;
                                             X          -> X
                                         end,
                                size   = Size},
    case MsgBin of
        <<>> -> {MsgId, Props};
        _    -> Msg = #basic_message{id = MsgId} = binary_to_term(MsgBin),
                {Msg, Props}
    end.

%% An incremental transform on the binary data used during upgrades
-spec store_msg_segment(binary()) -> {binary(), binary()}.
store_msg_segment(<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1,
                    RelSeq:?REL_SEQ_BITS, MsgId:?MSG_ID_BITS,
                    Expiry:?EXPIRY_BITS, Size:?SIZE_BITS, Rest/binary>>) ->
    {<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1, RelSeq:?REL_SEQ_BITS,
       MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS, Size:?SIZE_BITS,
       0:?EMBEDDED_SIZE_BITS>>, Rest};
store_msg_segment(<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                    RelSeq:?REL_SEQ_BITS, Rest/binary>>) ->
    {<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS, RelSeq:?REL_SEQ_BITS>>,
     Rest};
store_msg_segment(_) ->
    stop.

%% An incremental transform on the binary data used during upgrades
-spec store_msg_size_segment(binary()) -> {binary(), binary()}.
store_msg_size_segment(<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1,
                         RelSeq:?REL_SEQ_BITS, MsgId:?MSG_ID_BITS,
                         Expiry:?EXPIRY_BITS, Rest/binary>>) ->
    {<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1, RelSeq:?REL_SEQ_BITS,
       MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS, 0:?SIZE_BITS>>, Rest};
store_msg_size_segment(<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                        RelSeq:?REL_SEQ_BITS, Rest/binary>>) ->
    {<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS, RelSeq:?REL_SEQ_BITS>>,
     Rest};
store_msg_size_segment(_) ->
    stop.

%% An incremental transform on the binary data used during upgrades
-spec add_queue_ttl_segment(binary()) -> {[binary()], binary()}.
add_queue_ttl_segment(<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1,
                        RelSeq:?REL_SEQ_BITS, MsgId:?MSG_ID_BYTES/binary,
                        Rest/binary>>) ->
    {[<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1, RelSeq:?REL_SEQ_BITS>>,
      MsgId, <<?NO_EXPIRY:?EXPIRY_BITS>>], Rest};
add_queue_ttl_segment(<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                        RelSeq:?REL_SEQ_BITS, Rest/binary>>) ->
    {<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS, RelSeq:?REL_SEQ_BITS>>,
     Rest};
add_queue_ttl_segment(_) ->
    stop.

%% An incremental transform on the binary data used during upgrades
-spec avoid_zeroes_segment(binary()) -> {binary(), binary()}.
avoid_zeroes_segment(<<?PUB_PREFIX:?PUB_PREFIX_BITS,  IsPersistentNum:1,
                       RelSeq:?REL_SEQ_BITS, MsgId:?MSG_ID_BITS,
                       Expiry:?EXPIRY_BITS, Rest/binary>>) ->
    {<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1, RelSeq:?REL_SEQ_BITS,
       MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS>>, Rest};
avoid_zeroes_segment(<<0:?REL_SEQ_ONLY_PREFIX_BITS,
                       RelSeq:?REL_SEQ_BITS, Rest/binary>>) ->
    {<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS, RelSeq:?REL_SEQ_BITS>>,
     Rest};
avoid_zeroes_segment(_) ->
    stop.
