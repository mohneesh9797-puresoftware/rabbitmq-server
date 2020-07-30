%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(journal_file).

-include("rabbit_queue_index.hrl").

-export([entries_fold/3,
         store_msg_journal/1,
         store_msg_size_journal/1,
         add_queue_ttl_journal/1]).

-type entry() :: 'del' | 'ack' | pub().

-spec entries_fold(Fun :: fun ((integer(), entry(), A) -> A),
                   Acc0 :: A,
                   JournalBin :: binary()) -> A.
entries_fold(Fun, Acc0, <<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                          Rest/binary>>) ->
    entries_fold(Fun, Fun(SeqId, del, Acc0), Rest);
entries_fold(Fun, Acc0, <<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                          Rest/binary>>) ->
    entries_fold(Fun, Fun(SeqId, ack, Acc0), Rest);
entries_fold(_, Acc0, <<0:?JPREFIX_BITS, 0:?SEQ_BITS,
                        0:?PUB_RECORD_SIZE_BYTES/unit:8, _/binary>>) ->
    %% Journal entry composed only of zeroes was probably
    %% produced during a dirty shutdown so stop reading
    Acc0;
entries_fold(Fun, Acc0, <<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                          Bin:?PUB_RECORD_BODY_BYTES/binary,
                          MsgSize:?EMBEDDED_SIZE_BITS, MsgBin:MsgSize/binary,
                          Rest/binary>>) ->
    IsPersistent = case Prefix of
                       ?PUB_PERSIST_JPREFIX -> true;
                       ?PUB_TRANS_JPREFIX   -> false
                   end,
    entries_fold(Fun, Fun(SeqId, {IsPersistent, Bin, MsgBin}, Acc0), Rest);
entries_fold(_, Acc0, _ErrOrEoF) ->
  Acc0.

%% An incremental transform on the binary data used during upgrades
-spec store_msg_journal(binary()) -> {binary(), binary()}.
store_msg_journal(<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                    Rest/binary>>) ->
    {<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
store_msg_journal(<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                    Rest/binary>>) ->
    {<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
store_msg_journal(<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                    MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS, Size:?SIZE_BITS,
                    Rest/binary>>) ->
    {<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS, MsgId:?MSG_ID_BITS,
       Expiry:?EXPIRY_BITS, Size:?SIZE_BITS,
       0:?EMBEDDED_SIZE_BITS>>, Rest};
store_msg_journal(_) ->
    stop.

%% An incremental transform on the binary data used during upgrades
-spec store_msg_size_journal(binary()) -> {binary(), binary()}.
store_msg_size_journal(<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>) ->
    {<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
store_msg_size_journal(<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>) ->
    {<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
store_msg_size_journal(<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                         MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS,
                         Rest/binary>>) ->
    {<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS, MsgId:?MSG_ID_BITS,
       Expiry:?EXPIRY_BITS, 0:?SIZE_BITS>>, Rest};
store_msg_size_journal(_) ->
    stop.

%% An incremental transform on the binary data used during upgrades
-spec add_queue_ttl_journal(binary()) -> {binary(), binary()}.
add_queue_ttl_journal(<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>) ->
    {<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
add_queue_ttl_journal(<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>) ->
    {<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
add_queue_ttl_journal(<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        MsgId:?MSG_ID_BYTES/binary, Rest/binary>>) ->
    {[<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, MsgId,
      <<?NO_EXPIRY:?EXPIRY_BITS>>], Rest};
add_queue_ttl_journal(_) ->
    stop.
