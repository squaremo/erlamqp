-module(amqp_codec).

-export([parse/1, parse/2, parse_type/1, parse_value/2, generate/2]).

-export_type([value/0, type/0, encoding/0, result/1, map/0]).

-type(state(Value) :: 'start' | fun ((binary()) -> result(Value))).

-type(result(Value) :: {'more', state(Value)}
                       | {'value', Value, binary()}).

%% Values as Erlang values
-type(value() :: number() | atom() | binary()
                 | 'true'| 'false' | 'null'
                 | {'decimal', binary()}
                 | {'char', char()}
                 | {'timestamp', -16#8000000000000000..16#7fffffffffffffff}
                 | {'uuid', <<_:16>>}
                 | {'utf8', binary()}
                 | map()
                 | list(value())
                 | {'described', value(), value()}).

%% This will do for now, but we may want to admit other kinds of map
%% data structure.
-type(map() :: {[{value(), value()}]}).

-type(type_also_encoding() :: 'null' | 'boolean' | 'ubyte' | 'ushort'
                              | 'uint' | 'ulong' | 'byte' | 'short'
                              | 'int' | 'long'
                              | 'char' | 'timestamp' | 'uuid').

-type(type() :: type_also_encoding()
                | 'binary' | 'string' | 'symbol'
                | 'float' | 'double'
                | 'list' | 'map' | {'array', type()}
                | 'decimal32' | 'decimal64' | 'decimal128'
                | {'described', value(), type()}).

-type(encoding() :: type_also_encoding()
                    | 'true' | 'false' | 'smalluint' | 'uint0'
                    | 'smallulong' | 'ulong0' | 'smallint' | 'smalllong'
                    | 'ieee754binary32' | 'ieee754binary64'
                    | 'ieee754decimal32' | 'ieee754decimal64'
                    | 'ieee754decimal128'
                    | 'vbin8' | 'vbin32' | 'str8utf8' | 'str32utf8'
                    | 'sym8' | 'sym32' | 'list0' | 'list8' | 'list32'
                    | 'map8' | 'map32' | 'array8' | 'array32').

%% Parse a whole type and value
-spec(parse/1 :: (binary()) -> result({type(), value()})).

%% Continue a parsing
-spec(parse/2 :: (state(Value), binary()) -> result(Value)).

%% Parse a type constructor
-spec(parse_type/1 :: (binary()) -> result({type(), encoding()})).

%% Parse a value given the type
-spec(parse_value/2 :: (encoding(), binary()) -> result(value())).

%% Generate bytes given a specific type and the value
-spec(generate/2 :: (type(), value()) -> iolist()).

parse(Bin) ->
    parse(start, Bin).

parse(start, Bin) ->
    case parse_type(Bin) of
        {value, {Type, Encoding}, Rest} ->
            continue(parse_value(Encoding, Rest),
                     fun(V, R) -> {value, {Type, V}, R} end);
        {more, _K} ->
            {more, fun (MoreBin) ->
                           %% bleh, just start again
                           parse(start, <<Bin/binary, MoreBin/binary>>)
                   end}
    end;
parse(K, Bin) ->
    K(Bin).

parse_type(<<>>) ->
    {more, fun parse_type/1};
%% 'Described' types
parse_type(<<16#00, Rest/binary>>) ->
    continue(parse(Rest),
             fun({_Type, Val}, Rest1) ->
                     continue(parse_type(Rest1),
                              fun ({_Type, Enc}, Rest2) ->
                                      {value, {{described, Val},
                                               {described, Val, Enc}}, Rest2}
                              end)
             end);
%% Arrays get special treatment. Because the type of the elements
%% comes in the common constructor after the size, we have to peek
%% ahead.
parse_type(Bin = <<16#e0, Size:8, Count:8, Rest/binary>>) ->
    continue(parse_type(Rest),
             fun({Type, _Enc}, _R) ->
                     <<_C:8, Rest1/binary>> = Bin,
                     {value, {{array, Type}, array8}, Rest1}
             end);
parse_type(Bin = <<16#f0, Size:32, Count:32, Rest/binary>>) ->
    continue(parse_type(Rest),
             fun({Type, Enc}, _R) ->
                     <<_C:8, Rest1/binary>> = Bin,
                     {value, {{array, Type}, array32}, Rest1}
             end); 
parse_type(Bin = <<Constructor:8, Rest/binary>>) when
      Constructor == 16#e0 orelse Constructor == 16#f0 ->
    {more, fun(Bin1) -> parse_type(<<Bin/binary, Bin1/binary>>) end};
%% Primitive types
parse_type(<<Constructor:8, Rest/binary>>) ->
    {value, type_and_encoding(Constructor), Rest}.

%% Zero width
parse_value(null, Rest) ->
    {value, null, Rest};
parse_value(true, Rest) ->
    {value, true, Rest};
parse_value(false, Rest) ->
    {value, false, Rest};
parse_value(uint0, Rest) ->
    {value, 0, Rest};
parse_value(ulong0, Rest) ->
    {value, 0, Rest};
parse_value(list0, Rest) ->
    {value, [], Rest};
%% Fixed width
parse_value(boolean, <<0, Rest/binary>>) ->
    {value, false, Rest};
parse_value(boolean, <<1, Rest/binary>>) ->
    {value, true, Rest};
parse_value(ubyte, <<Val:8/unsigned, Rest/binary>>) ->
    {value, Val, Rest};
parse_value(ushort, <<Val:16/unsigned, Rest/binary>>) ->
    {value, Val, Rest};
parse_value(uint, <<Val:32/unsigned, Rest/binary>>) ->
    {value, Val, Rest};
parse_value(smalluint, <<Val:8/unsigned, Rest/binary>>) ->
    {value, Val, Rest};
parse_value(ulong, <<Val:64/unsigned, Rest/binary>>) ->
    {value, Val, Rest};
parse_value(smallulong, <<Val:8/unsigned, Rest/binary>>) ->
    {value, Val, Rest};
parse_value(byte, <<Val:8/signed, Rest/binary>>) ->
    {value, Val, Rest};
parse_value(short, <<Val:16/signed, Rest/binary>>) ->
    {value, Val, Rest};
parse_value(int, <<Val:32/signed, Rest/binary>>) ->
    {value, Val, Rest};
parse_value(smallint, <<Val:8/signed, Rest/binary>>) ->
    {value, Val, Rest};
parse_value(long, <<Val:64/signed, Rest/binary>>) ->
    {value, Val, Rest};
parse_value(smalllong, <<Val:8/signed, Rest/binary>>) ->
    {value, Val, Rest};
parse_value(ieee754binary32, <<Val:32/float, Rest/binary>>) ->
    {value, Val, Rest};
parse_value(ieee754binary64, <<Val:64/float, Rest/binary>>) ->
    {value, Val, Rest};
%% Decimals: left as opaque binaries for now. Not used in the
%% protocol.
parse_value(ieee754decimal32, <<Val:32, Rest/binary>>) ->
    {value, {decimal, Val}, Rest};
parse_value(ieee754decimal64, <<Val:64, Rest/binary>>) ->
    {value, {decimal, Val}, Rest};
parse_value(ieee754decimal128, <<Val:128, Rest/binary>>) ->
    {value, {decimal, Val}, Rest};
parse_value(utf32, <<Val/utf32, Rest/binary>>) ->
    {value, {char, Val}, Rest};
parse_value(ms64, <<Val:64/signed, Rest/binary>>) ->
    {value, {timestamp, Val}, Rest};
parse_value(uuid, <<Val:16/binary, Rest/binary>>) ->
    {value, {uuid, Val}, Rest};
%% Variable width
parse_value(vbin8, <<Size:8, Val:Size/binary, Rest/binary>>) ->
    {value, Val, Rest};
parse_value(vbin32, <<Size:32, Val:Size/binary, Rest/binary>>) ->
    {value, Val, Rest};
parse_value(str8utf8, <<Size:8, Val:Size/binary, Rest/binary>>) ->
    {value, {utf8, Val}, Rest};
parse_value(str32utf8, <<Size:32, Val:Size/binary, Rest/binary>>) ->
    {value, {utf8, Val}, Rest};
%% Atoms aren't collected. An alternative would be to use strings, but
%% they look like lists; or wrap in a tuple.
parse_value(sym8, <<Size:8, Chars:Size/binary, Rest/binary>>) ->
    {value, list_to_atom(binary_to_list(Chars)), Rest};
%% Given the use of symbol it's ridiculous to have anything longer than
%% 8 bits, or at a stretch 16 bits, worth. Sigh.
%% FIXME Use something other than an atom.
parse_value(sym32, <<Size:32, Chars:Size/binary, Rest/binary>>) ->
    {value, list_to_atom(binary_to_list(Chars)), Rest};
%% Compound types. Extra for experts: don't require the whole value
%% before giving a result, since we may have to allocate large
%% binaries to be able to parse all at once.
parse_value(list8, <<Size:8, Encoded:Size/binary, Rest/binary>>) ->
    parse_list(8, Encoded, valueK(Rest));
parse_value(list32, <<Size:32, Encoded:Size/binary, Rest/binary>>) ->
    parse_list(32, Encoded, valueK(Rest));
parse_value(map8, <<Size:8, Encoded:Size/binary, Rest/binary>>) ->
    parse_list(8, Encoded, valueK(fun mapify/1, Rest));
parse_value(map32, <<Size:32, Encoded:Size/binary, Rest/binary>>) ->
    parse_list(32, Encoded, valueK(fun mapify/1, Rest));
parse_value(array8, <<Size:8, Encoded:Size/binary, Rest/binary>>) ->
    parse_array(8, Encoded, valueK(Rest));
parse_value(array32, <<Size:32, Encoded:Size/binary, Rest/binary>>) ->
    parse_array(32, Encoded, valueK(Rest));
parse_value({described, Name, Enc}, Bin) ->
    continue(parse_value(Enc, Bin),
             fun(Val, Rest) -> {value, {described, Name, Val}, Rest} end); 

%% Fall-through -- we assume it's because the bit pattern has failed,
%% so there are not sufficient bytes available to parse a whole value.
%% TODO check the encoding is correct.
parse_value(Encoding, Insufficient) ->
    {more, fun (Bin) ->
                   parse_value(Encoding, <<Insufficient/binary, Bin/binary>>)
           end}.

generate(null, null) ->
    [16#40];
generate(boolean, true) ->
    [16#41];
generate(boolean, false) ->
    [16#42];
generate(ubyte, Num) ->
    [16#50, Num];
generate(ushort, Num) ->
    <<16#60, Num:16>>;
generate(uint, Num) ->
    if Num == 0  -> [16#43];
       Num < 256 -> <<16#52, Num:8>>;
       true      -> <<16#70, Num:32>>
    end;
generate(ulong, Num) ->
    if Num == 0  -> [16#44]; 
       Num < 256 -> <<16#53, Num:8>>;
       true      -> <<16#80, Num:64>>
    end;
generate(byte, Num) ->
    <<16#51, Num:8/signed>>;
generate(short, Num) ->
    <<16#61, Num:16/signed>>;
generate(int, Num) ->
    if Num < 128 andalso Num > -129 ->
            <<16#54, Num:8/signed>>;
       true ->
            <<16#71, Num:32/signed>>
    end;
generate(long, Num) ->
    if Num < 128 andalso Num > -129 ->
            <<16#55, Num:8/signed>>;
       true ->
            <<16#81, Num:64/signed>>
    end;
generate(float, Num) ->
    <<16#72, Num:32/float>>;
generate(double, Num) ->
    <<16#82, Num:64/float>>;
generate(decimal32, {decimal, DecBin = <<_:32>>}) ->
    [16#74, DecBin];
generate(decimal64, {decimal, DecBin = <<_:64>>}) ->
    [16#84, DecBin];
generate(decimal128, {decimal, DecBin = <<_:128>>}) ->
    [16#94, DecBin];
generate(char, {char, CharBin = <<_/utf32>>}) ->
    [16#73, CharBin];
generate(timestamp, {timestamp, TS}) ->
    <<16#83, TS:64/signed>>;
generate(uuid, {uuid, Bin = <<_:16/binary>>}) ->
    [16#98, Bin];
generate(binary, Bin) ->
    Size = size(Bin),
    if Size < 256 ->
            [16#a0, Size, Bin];
       true ->
            [<<16#b0, Size:32>>, Bin]
    end;
generate(string, {utf8, Bin}) ->
    Size = size(Bin),
    if Size < 256 ->
            [16#a1, Size, Bin];
       true ->
            [<<16#b1, Size:32>>, Bin]
    end;
%% Erlang atoms are not bigger than 255 chars
generate(symbol, Sym) when is_atom(Sym) ->
    SymList = atom_to_list(Sym),
    <<16#a3, (length(SymList)):8, (list_to_binary(SymList))/binary>>;
generate(list, List) ->
    generate_list(List);
generate(map, Pairs) ->
    generate_map(Pairs);
generate({array, Type}, Vals) ->
    generate_array(Type, Vals).


generate_list([]) ->
    [16#45];
generate_list(List) ->
    [].

generate_map(Pairs) ->
    [].

generate_array(Type, Entries) ->
    [].

%% ----- Helpers


valueK(Rest) ->
    fun (Value) -> {value, Value, Rest} end.

valueK(X, Rest) ->
    fun (Value) -> {value, X(Value), Rest} end.

mapify(FlatList) ->
    mapify1(FlatList, []).

mapify1([], Acc) ->
    {lists:reverse(Acc)}; %% TODO are maps supposed to be ordered bags?
mapify1([Key, Value | Rest], Acc) ->
    mapify1(Rest, [{Key, Value} | Acc]).

parse_list(Unit, Bin, K) ->
    <<Count:Unit, Values/binary>> = Bin,
    parse_list1(Count, Values, [], K).

%% OMG not tail call -- lists, arrays or maps nested thousands deep
%% could blow the stack!
parse_list1(0, <<>>, Acc, K) ->
    K(lists:reverse(Acc));
parse_list1(Count, Bin, Acc, K) ->
    {value, {_Type, Val}, Rest1} = parse(Bin),
    parse_list1(Count - 1, Rest1, [Val | Acc], K).

parse_array(Unit, Bin, K) ->
    <<Count:Unit, ConstructorAndValue/binary>> = Bin,
    {value, {_Type, Encoding}, Rest} = parse_type(ConstructorAndValue),
    parse_array1(Count, Encoding, Rest, [], K).

parse_array1(0, _Encoding, <<>>, Acc, K) ->
    K(lists:reverse(Acc));
%% Could cons the array straight away for 1-byte encodings, but it
%% doesn't save all that much (how many arrays of null will there be?)
parse_array1(Count, Encoding, Bin, Acc, K) ->
    {value, Val, Rest} = parse_value(Encoding, Bin),
    parse_array1(Count - 1, Encoding, Rest, [Val | Acc], K).

continue(Parsed, K) ->
    case Parsed of
        {value, Value, Rest1} ->
            K(Value, Rest1);
        {more, Parse1} ->
            {more, fun (MoreBin) ->
                           continue(Parse1(MoreBin), K)
                   end}
    end.


type_and_encoding(16#40) -> {null, null};
type_and_encoding(16#56) -> {boolean, boolean};
type_and_encoding(16#41) -> {boolean, true};
type_and_encoding(16#42) -> {boolean, false};
type_and_encoding(16#50) -> {ubyte, ubyte};
type_and_encoding(16#60) -> {ushort, ushort};
type_and_encoding(16#70) -> {uint, uint};
type_and_encoding(16#52) -> {uint, smalluint};
type_and_encoding(16#43) -> {uint, uint0};
type_and_encoding(16#80) -> {ulong, ulong};
type_and_encoding(16#53) -> {ulong, smallulong};
type_and_encoding(16#44) -> {ulong, ulong0};
type_and_encoding(16#51) -> {byte, byte};
type_and_encoding(16#61) -> {short, short};
type_and_encoding(16#71) -> {int, int};
type_and_encoding(16#54) -> {int, smallint};
type_and_encoding(16#81) -> {long, long};
type_and_encoding(16#55) -> {long, smalllong};
type_and_encoding(16#72) -> {float, ieee754binary32};
type_and_encoding(16#82) -> {double, ieee754binary64};
type_and_encoding(16#74) -> {decimal32, ieee754decimal32};
type_and_encoding(16#84) -> {decimal64, ieee754decimal64};
type_and_encoding(16#94) -> {decimal128, ieee754decimal128};
type_and_encoding(16#73) -> {char, utf32};
type_and_encoding(16#83) -> {timestamp, ms64};
type_and_encoding(16#98) -> {uuid, uuid};
type_and_encoding(16#a0) -> {binary, vbin8};
type_and_encoding(16#b0) -> {binary, vbin32};
type_and_encoding(16#a1) -> {string, str8utf8};
type_and_encoding(16#b1) -> {string, str32utf8};
type_and_encoding(16#a3) -> {symbol, sym8};
type_and_encoding(16#b3) -> {symbol, sym32};
type_and_encoding(16#45) -> {list, list0};
type_and_encoding(16#c0) -> {list, list8};
type_and_encoding(16#d0) -> {list, list32};
type_and_encoding(16#c1) -> {map, map8};
type_and_encoding(16#d1) -> {map, map32}.
