-module(seq2).

-export([ofSparseArrayReversed/1]).

%% Returns a seq of {Index, Value} tuples from the sparse array, from
%% the highest index
-spec ofSparseArrayReversed(array:array()) -> seq:seq().
ofSparseArrayReversed(Array) ->
    Default = array:default(Array),
    seq:unfold(
      fun (I) ->
              (fun Unfold1(FromIndex) ->
                       if
                           FromIndex >= 0 ->
                               case array:get(FromIndex, Array) of
                                   Default ->
                                       Unfold1(FromIndex - 1);
                                   Value ->
                                       {{FromIndex, Value}, FromIndex - 1}
                               end;
                           true ->
                               undefined
                       end
               end)(I)
      end, array:size(Array) - 1).
