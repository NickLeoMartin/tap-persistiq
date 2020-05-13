# streams: API URL endpoints to be called
# properties:
#   <root node>: Plural stream name for the endpoint
#   path: API endpoint relative path, when added to the base URL, creates the full path,
#       default = stream_name
#   key_properties: Primary key fields for identifying an endpoint record.
#   replication_method: INCREMENTAL or FULL_TABLE
#   replication_keys: bookmark_field(s), typically a date-time, used for filtering the results
#        and setting the state
#   params: Query, sort, and other endpoint specific parameters; default = {}
#   data_key: JSON element containing the results list for the endpoint; default = 'results'
#   bookmark_query_field: From date-time field used for filtering the query
#   bookmark_type: Data type for bookmark, integer or datetime

# Notes:
# - leads endpoint is problematic & using start_date in config
#   to limit full-table replication.
STREAMS = {
    'users': {
        'path': 'users',
        'data_key': 'users',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE'
    },
    'leads': {
        'path': 'leads',
        'data_key': 'leads',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'bookmark_query_field': 'updated_after',
        'bookmark_type': 'datetime'
    },
    'campaigns': {
        'path': 'campaigns',
        'data_key': 'campaigns',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE'
    }
}

def flatten_streams():
    flat_streams = {}
    for stream_name, endpoint_config in STREAMS.items():
        flat_streams[stream_name] = {
            'key_properties': endpoint_config.get('key_properties'),
            'replication_method': endpoint_config.get('replication_method'),
            'replication_keys': endpoint_config.get('replication_keys')
        }
    return flat_streams
