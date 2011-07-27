-module(parse_test).

-include_lib("eunit/include/eunit.hrl").


byte_by_byte(C) ->
    lists:foldl(fun (Byte, {more, S}) ->
                        amqp_codec:parse(S, Byte)
                end, {more, start},
                [<<B>> || B <- binary_to_list(C)]).

parse_cases(Cases) ->
    Trailing = <<"and extra goes here">>,
    [{A, fun() ->
                 {value, B, <<>>} = amqp_codec:parse(C),
                 {value, B, Trailing} =
                     amqp_codec:parse(<<C/binary, Trailing/binary>>),
                 {value, B, <<>>} = byte_by_byte(C)
         end} ||
        {A, B, C} <- Cases].


parse_primitive_test_() ->
    parse_cases(
      [{"null", {null, null}, <<16#40>>},
       {"boolean", {boolean, true}, <<16#56, 1>>},
       {"boolean", {boolean, false}, <<16#56, 0>>},
       {"true", {boolean, true}, <<16#41>>},
       {"false", {boolean, false}, <<16#42>>},
       {"ubyte", {ubyte, 251}, <<16#50, 251>>},
       {"ushort", {ushort, 16#fffe}, <<16#60, 255, 254>>},
       {"uint", {uint, 16#fffffffe}, <<16#70, 255, 255, 255, 254>>},
       {"smalluint", {uint, 255}, <<16#52, 255>>},
       {"uint0", {uint, 0}, <<16#43>>},
       {"ulong", {ulong, 16#fffffffffffffffe},
        <<16#80, 255, 255, 255, 255, 255, 255, 255, 254>>},
       {"smallulong", {ulong, 254}, <<16#53, 254>>},
       {"ulong0", {ulong, 0}, <<16#44>>},
       {"byte", {byte, 127}, <<16#51, 127>>},
       {"negbyte", {byte, -1}, <<16#51, 255>>},
       {"short", {short, 16#7fff}, <<16#61, 127, 255>>},
       {"negshort", {short, -1}, <<16#61, 255, 255>>},
       {"int", {int, 16#7fffffff}, <<16#71, 127, 255, 255, 255>>},
       {"int", {int, -1}, <<16#71, 255, 255, 255, 255>>},
       {"smallint", {int, -128}, <<16#54, 128>>},
       {"long", {long, 16#7fffffffffffffff},
        <<16#81, 127, 255, 255, 255, 255, 255, 255, 255>>},
       {"neg long", {long, -1},
        <<16#81, 255, 255, 255, 255, 255, 255, 255, 255>>},
       {"smalllong", {long, -1}, <<16#55, 255>>},

       %% TODO Floats, Doubles, decimals

       {"char", {char, {char, 16#1f407}}, <<16#73, 16#1f407/utf32>>},
       {"timestamp", {timestamp, {timestamp, 16#c0ffee}},
        <<16#83, 16#c0ffee:64/signed>>},
       {"uuid", {uuid, {uuid, <<"completelyrandom">>}},
        <<16#98, "completelyrandom">>},
       {"binary8", {binary, <<"short value">>}, <<16#a0, 11, "short value">>},
       {"binary32", {binary, <<"long value", 0:255/unit:8>>},
        <<16#b0, (10 + 255):32, "long value", 0:255/unit:8>>},
       {"string8", {string, {utf8, <<"short value ", 16#f09f9087:32>>}},
        <<16#a1, 16, "short value ", 16#f09f9087:32>>},
       {"string32",
        {string, {utf8, list_to_binary(lists:duplicate(255, $x))}},
        <<16#b1, 255:32, (list_to_binary(lists:duplicate(255, $x)))/binary>>},
       {"symbol8", {symbol, foobar}, <<16#a3, 6, "foobar">>},
       {"symbol32", {symbol, foobar}, <<16#b3, 6:32, "foobar">>}
      ]).

parse_lists_test_() ->
    parse_cases(
      [
       {"list0", {list, []}, <<16#45>>},
       {"empty list8", {list, []}, <<16#c0, 1, 0>>},
       {"empty list32", {list, []}, <<16#d0, 4:32, 0:32>>},
       {"flat list8", {list, [true, false]},
        <<16#c0, 3, 2, 16#41, 16#42>>},
       {"nested list8", {list, [10, [20]]},
        <<16#c0, 8, 2, 16#50, 10, 16#c0, 3, 1, 16#50, 20>>},
       {"nested nested list8",
        {list, [[[[]]]]},
        <<16#c0, 8, 1, 16#c0, 5, 1, 16#c0, 2, 1, 16#45>>},
       {"list32", {list, lists:seq(1, 16#fff)},
        <<16#d0, (4 + 3 * 16#fff):32, 16#fff:32,
          (list_to_binary(lists:map(fun (N) -> [16#60, N bsr 8, N band 255] end,
                                    lists:seq(1, 16#fff))))/binary>>}
      ]).

parse_maps_test_() ->
    parse_cases(
      [
       {"map8", {map, {[{foo, 5}]}},
        <<16#c1, 8, 2, 16#a3, 3, "foo", 16#50, 5>>}
      ]).

parse_arrays_test_() ->
    parse_cases(
      [
       {"array8", {{array, ubyte}, [1, 2]}, <<16#e0, 4, 2, 16#50, 1, 2>>},
       {"array nulls", {{array, null}, [null, null, null]}, <<16#e0, 2, 3, 16#40>>}
      ]).

parse_described_test_() ->
    parse_cases(
      [
       {"URL", {{described, 'URL'},
                {described, 'URL', {utf8, <<"http://rabbit.mq/">>}}},
        <<16#00, 16#a3, 3, "URL", 16#a1, 17, "http://rabbit.mq/">>}
      ]).

roundtrip_test_() ->
    [{N, fun() ->
                 B = iolist_to_binary(amqp_codec:generate(T, V)),
                 {value, {T, V}, <<>>} = amqp_codec:parse(B),
                 B1 = iolist_to_binary(amqp_codec:generate(V)),
                 {value, {_, V}, <<>>} = amqp_codec:parse(B1)
         end} ||
        {N, T, V} <-
            [{"null", null, null},
             {"true", boolean, true},
             {"false", boolean, false},
             {"true sym", symbol, true},
             {"ubyte", ubyte, 253},
             {"ushort", ushort, 16#beef},
             {"uint0", uint, 0},
             {"smalluint", uint, 255},
             {"uint", uint, 16#c0ffee},
             {"ulong0", ulong, 0},
             {"smallulong", ulong, 237},
             %% Floats doubles and decimals
             %% ...
             {"uuid", uuid, {uuid, <<"is sixteen bytes">>}},
             {"timestamp", timestamp, {timestamp, -16#8000000000000000}},
             {"string", string, {utf8, <<"a short string">>}},
             {"symbol", symbol, a_short_symbol},
             %% Longer symbols and strings and binaries
             %% ...
             %% Lists, maps
             {"empty list", list, []},
             {"list of numbers", list, [1,2,3,4]},
             {"map", map, {[{foo, 3}, {bar, 6}]}},
             %% Arrays
             {"array", {array, int}, [0, 5, 257]},
             {"array bin", {array, binary}, [<<"foo">>, <<"bar">>]},
             {"array list", {array, list}, [[1, 2, 3], [4, 5, 6]]},
             {"array map", {array, map},
              [{[{foo, 1}, {bar, 2}]}, {[{baz, 3}]}]},
             {"array null", {array, null}, [null, null, null, null]}
            ]].

%% TODO arrays of arrays
