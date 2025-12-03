<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Supported Spark Expressions

### agg_funcs
 - [ ] any
 - [ ] any_value
 - [ ] approx_count_distinct
 - [ ] approx_percentile
 - [ ] array_agg
 - [ ] avg
 - [ ] bit_and
 - [ ] bit_or
 - [ ] bit_xor
 - [ ] bitmap_construct_agg
 - [ ] bitmap_or_agg
 - [ ] bool_and
 - [ ] bool_or
 - [ ] collect_list
 - [ ] collect_set
 - [ ] corr
 - [ ] count
 - [ ] count_if
 - [ ] count_min_sketch
 - [ ] covar_pop
 - [ ] covar_samp
 - [ ] every
 - [ ] first
 - [ ] first_value
 - [ ] grouping
 - [ ] grouping_id
 - [ ] histogram_numeric
 - [ ] hll_sketch_agg
 - [ ] hll_union_agg
 - [ ] kurtosis
 - [ ] last
 - [ ] last_value
 - [ ] max
 - [ ] max_by
 - [ ] mean
 - [ ] median
 - [ ] min
 - [ ] min_by
 - [ ] mode
 - [ ] percentile
 - [ ] percentile_approx
 - [ ] regr_avgx
 - [ ] regr_avgy
 - [ ] regr_count
 - [ ] regr_intercept
 - [ ] regr_r2
 - [ ] regr_slope
 - [ ] regr_sxx
 - [ ] regr_sxy
 - [ ] regr_syy
 - [ ] skewness
 - [ ] some
 - [ ] std
 - [ ] stddev
 - [ ] stddev_pop
 - [ ] stddev_samp
 - [ ] sum
 - [ ] try_avg
 - [ ] try_sum
 - [ ] var_pop
 - [ ] var_samp
 - [ ] variance

### array_funcs
 - [ ] array
 - [ ] array_append
 - [ ] array_compact
 - [ ] array_contains
 - [ ] array_distinct
 - [ ] array_except
 - [ ] array_insert
 - [ ] array_intersect
 - [ ] array_join
 - [ ] array_max
 - [ ] array_min
 - [ ] array_position
 - [ ] array_prepend
 - [ ] array_remove
 - [ ] array_repeat
 - [ ] array_union
 - [ ] arrays_overlap
 - [ ] arrays_zip
 - [ ] flatten
 - [ ] get
 - [ ] sequence
 - [ ] shuffle
 - [ ] slice
 - [ ] sort_array

### bitwise_funcs
 - [ ] &
 - [ ] ^
 - [ ] bit_count
 - [ ] bit_get
 - [ ] getbit
 - [ ] shiftright
 - [ ] shiftrightunsigned
 - [ ] |
 - [ ] ~

### collection_funcs
 - [ ] array_size
 - [ ] cardinality
 - [ ] concat
 - [ ] reverse
 - [ ] size

### conditional_funcs
 - [ ] coalesce
 - [ ] if
 - [ ] ifnull
 - [ ] nanvl
 - [ ] nullif
 - [ ] nvl
 - [ ] nvl2
 - [ ] when

### conversion_funcs
 - [ ] bigint
 - [ ] binary
 - [ ] boolean
 - [ ] cast
 - [ ] date
 - [ ] decimal
 - [ ] double
 - [ ] float
 - [ ] int
 - [ ] smallint
 - [ ] string
 - [ ] timestamp
 - [ ] tinyint

### csv_funcs
 - [ ] from_csv
 - [ ] schema_of_csv
 - [ ] to_csv

### datetime_funcs
 - [ ] add_months
 - [ ] convert_timezone
 - [ ] curdate
 - [ ] current_date
 - [ ] current_timestamp
 - [ ] current_timezone
 - [ ] date_add
 - [ ] date_diff
 - [ ] date_format
 - [ ] date_from_unix_date
 - [ ] date_part
 - [ ] date_sub
 - [ ] date_trunc
 - [ ] dateadd
 - [ ] datediff
 - [ ] datepart
 - [ ] day
 - [ ] dayofmonth
 - [ ] dayofweek
 - [ ] dayofyear
 - [ ] extract
 - [ ] from_unixtime
 - [ ] from_utc_timestamp
 - [ ] hour
 - [ ] last_day
 - [ ] localtimestamp
 - [ ] make_date
 - [ ] make_dt_interval
 - [ ] make_interval
 - [ ] make_timestamp
 - [ ] make_timestamp_ltz
 - [ ] make_timestamp_ntz
 - [ ] make_ym_interval
 - [ ] minute
 - [ ] month
 - [ ] months_between
 - [ ] next_day
 - [ ] now
 - [ ] quarter
 - [ ] second
 - [ ] timestamp_micros
 - [ ] timestamp_millis
 - [ ] timestamp_seconds
 - [ ] to_date
 - [ ] to_timestamp
 - [ ] to_timestamp_ltz
 - [ ] to_timestamp_ntz
 - [ ] to_unix_timestamp
 - [ ] to_utc_timestamp
 - [ ] trunc
 - [ ] try_to_timestamp
 - [ ] unix_date
 - [ ] unix_micros
 - [ ] unix_millis
 - [ ] unix_seconds
 - [ ] unix_timestamp
 - [ ] weekday
 - [ ] weekofyear
 - [ ] year

### generator_funcs
 - [ ] explode
 - [ ] explode_outer
 - [ ] inline
 - [ ] inline_outer
 - [ ] posexplode
 - [ ] posexplode_outer
 - [ ] stack

### hash_funcs
 - [ ] crc32
 - [ ] hash
 - [ ] md5
 - [ ] sha
 - [ ] sha1
 - [ ] sha2
 - [ ] xxhash64

### json_funcs
 - [ ] from_json
 - [ ] get_json_object
 - [ ] json_array_length
 - [ ] json_object_keys
 - [ ] json_tuple
 - [ ] schema_of_json
 - [ ] to_json

### lambda_funcs
 - [ ] aggregate
 - [ ] array_sort
 - [ ] exists
 - [ ] filter
 - [ ] forall
 - [ ] map_filter
 - [ ] map_zip_with
 - [ ] reduce
 - [ ] transform
 - [ ] transform_keys
 - [ ] transform_values
 - [ ] zip_with

### map_funcs
 - [ ] element_at
 - [ ] map
 - [ ] map_concat
 - [ ] map_contains_key
 - [ ] map_entries
 - [ ] map_from_arrays
 - [ ] map_from_entries
 - [ ] map_keys
 - [ ] map_values
 - [ ] str_to_map
 - [ ] try_element_at

### math_funcs
 - [ ] %
 - [ ] *
 - [ ] +
 - [ ] -
 - [ ] /
 - [ ] abs
 - [ ] acos
 - [ ] acosh
 - [ ] asin
 - [ ] asinh
 - [ ] atan
 - [ ] atan2
 - [ ] atanh
 - [ ] bin
 - [ ] bround
 - [ ] cbrt
 - [ ] ceil
 - [ ] ceiling
 - [ ] conv
 - [ ] cos
 - [ ] cosh
 - [ ] cot
 - [ ] csc
 - [ ] degrees
 - [ ] div
 - [ ] e
 - [ ] exp
 - [ ] expm1
 - [ ] factorial
 - [ ] floor
 - [ ] greatest
 - [ ] hex
 - [ ] hypot
 - [ ] least
 - [ ] ln
 - [ ] log
 - [ ] log10
 - [ ] log1p
 - [ ] log2
 - [ ] mod
 - [ ] negative
 - [ ] pi
 - [ ] pmod
 - [ ] positive
 - [ ] pow
 - [ ] power
 - [ ] radians
 - [ ] rand
 - [ ] randn
 - [ ] random
 - [ ] rint
 - [ ] round
 - [ ] sec
 - [ ] shiftleft
 - [ ] sign
 - [ ] signum
 - [ ] sin
 - [ ] sinh
 - [ ] sqrt
 - [ ] tan
 - [ ] tanh
 - [ ] try_add
 - [ ] try_divide
 - [ ] try_multiply
 - [ ] try_subtract
 - [ ] unhex
 - [ ] width_bucket

### misc_funcs
 - [ ] aes_decrypt
 - [ ] aes_encrypt
 - [ ] assert_true
 - [ ] bitmap_bit_position
 - [ ] bitmap_bucket_number
 - [ ] bitmap_count
 - [ ] equal_null
 - [ ] hll_sketch_estimate
 - [ ] hll_union
 - [ ] input_file_block_length
 - [ ] input_file_block_start
 - [ ] input_file_name
 - [ ] monotonically_increasing_id
 - [ ] raise_error
 - [ ] spark_partition_id
 - [ ] try_aes_decrypt
 - [ ] typeof
 - [ ] user
 - [ ] uuid
 - [ ] version

### predicate_funcs
 - [ ] !
 - [ ] <
 - [ ] <=
 - [ ] <=>
 - [ ] =
 - [ ] ==
 - [ ] >
 - [ ] >=
 - [ ] and
 - [ ] ilike
 - [ ] in
 - [ ] isnan
 - [ ] isnotnull
 - [ ] isnull
 - [ ] like
 - [ ] not
 - [ ] or
 - [ ] regexp
 - [ ] regexp_like
 - [ ] rlike

### string_funcs
 - [ ] ascii
 - [ ] base64
 - [ ] bit_length
 - [ ] btrim
 - [ ] char
 - [ ] char_length
 - [ ] character_length
 - [ ] chr
 - [ ] concat_ws
 - [ ] contains
 - [ ] decode
 - [ ] elt
 - [ ] encode
 - [ ] endswith
 - [ ] find_in_set
 - [ ] format_number
 - [ ] format_string
 - [ ] initcap
 - [ ] instr
 - [ ] lcase
 - [ ] left
 - [ ] len
 - [ ] length
 - [ ] levenshtein
 - [ ] locate
 - [ ] lower
 - [ ] lpad
 - [ ] ltrim
 - [ ] luhn_check
 - [ ] mask
 - [ ] octet_length
 - [ ] overlay
 - [ ] position
 - [ ] printf
 - [ ] regexp_count
 - [ ] regexp_extract
 - [ ] regexp_extract_all
 - [ ] regexp_instr
 - [ ] regexp_replace
 - [ ] regexp_substr
 - [ ] repeat
 - [ ] replace
 - [ ] right
 - [ ] rpad
 - [ ] rtrim
 - [ ] sentences
 - [ ] soundex
 - [ ] space
 - [ ] split
 - [ ] split_part
 - [ ] startswith
 - [ ] substr
 - [ ] substring
 - [ ] substring_index
 - [ ] to_binary
 - [ ] to_char
 - [ ] to_number
 - [ ] to_varchar
 - [ ] translate
 - [ ] trim
 - [ ] try_to_binary
 - [ ] try_to_number
 - [ ] ucase
 - [ ] unbase64
 - [ ] upper

### struct_funcs
 - [ ] named_struct
 - [ ] struct

### url_funcs
 - [ ] parse_url
 - [ ] url_decode
 - [ ] url_encode

### window_funcs
 - [ ] cume_dist
 - [ ] dense_rank
 - [ ] lag
 - [ ] lead
 - [ ] nth_value
 - [ ] ntile
 - [ ] percent_rank
 - [ ] rank
 - [ ] row_number

### xml_funcs
 - [ ] xpath
 - [ ] xpath_boolean
 - [ ] xpath_double
 - [ ] xpath_float
 - [ ] xpath_int
 - [ ] xpath_long
 - [ ] xpath_number
 - [ ] xpath_short
 - [ ] xpath_string