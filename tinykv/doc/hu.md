1   三种column family的数据结构
                            Key                      Value
    Data               key, startTs                 value
    Lock                key                         start_ts, primary_key, ttl
    Write              key, commitTs                start_ts [, short_value]