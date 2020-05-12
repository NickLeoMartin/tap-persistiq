def transform_json(this_json, stream_name, data_key):
    new_json = this_json

    if data_key in new_json:
        return new_json[data_key]

    return new_json
