%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(journal_file).

-export([entries_fold/3]).

-define(JOURNAL_FILENAME, "journal.jif").
-define(QUEUE_NAME_STUB_FILE, ".queue_name").

-define(PUB_PERSIST_JPREFIX, 2#00).
-define(PUB_TRANS_JPREFIX,   2#01).
-define(DEL_JPREFIX,         2#10).
-define(ACK_JPREFIX,         2#11).
-define(JPREFIX_BITS, 2).
-define(SEQ_BYTES, 8).
-define(SEQ_BITS, ((?SEQ_BYTES * 8) - ?JPREFIX_BITS)).

-define(SIZE_BYTES, 4).

-define(EMBEDDED_SIZE_BYTES, 4).
-define(EMBEDDED_SIZE_BITS, (?EMBEDDED_SIZE_BYTES * 8)).

-define(EXPIRY_BYTES, 8).

-define(MSG_ID_BYTES, 16). %% md5sum is 128 bit or 16 bytes

%% 16 bytes for md5sum + 8 for expiry
-define(PUB_RECORD_BODY_BYTES, (?MSG_ID_BYTES + ?EXPIRY_BYTES + ?SIZE_BYTES)).
%% + 4 for size
-define(PUB_RECORD_SIZE_BYTES, (?PUB_RECORD_BODY_BYTES + ?EMBEDDED_SIZE_BYTES)).

-type entry() :: 'del' | 'ack' | {IsPersistent :: boolean(),
                                  Bin :: binary(),
                                  MsgBin :: binary()}.

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
