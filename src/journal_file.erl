%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(journal_file).

-include("rabbit_queue_index.hrl").

-export([entries_fold/3]).

-type entry() :: 'del' | 'ack' | pub().

-spec entries_fold(Fun :: fun ((<<_:?SEQ_BITS>>, entry(), A) -> A),
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
