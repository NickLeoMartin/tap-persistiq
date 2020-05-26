import time
import math
import singer
from singer import metrics, metadata, Transformer, utils, UNIX_SECONDS_INTEGER_DATETIME_PARSING
from singer.utils import strptime_to_utc
from tap_persistiq.transform import transform_json
from tap_persistiq.streams import STREAMS

LOGGER = singer.get_logger()


def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()

    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.info('OS Error writing schema for: {}'.format(stream_name))
        raise err


def write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.info('OS Error writing record for: {}'.format(stream_name))
        LOGGER.info('record: {}'.format(record))
        raise err


def get_bookmark(state, stream, default):
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, default)
    )


def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Write state for stream: {}, value: {}'.format(stream, value))
    singer.write_state(state)


def transform_datetime(this_dttm):
    with Transformer() as transformer:
        new_dttm = transformer._transform_datetime(this_dttm)
    return new_dttm


def process_records(catalog, #pylint: disable=too-many-branches
                    stream_name,
                    records,
                    time_extracted,
                    bookmark_field=None,
                    bookmark_type=None,
                    max_bookmark_value=None,
                    last_datetime=None,
                    last_integer=None,
                    parent=None,
                    parent_id=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            # If child object, add parent_id to record
            if parent_id and parent:
                record[parent + '_id'] = parent_id

            # Transform record for Singer.io
            with Transformer(integer_datetime_fmt=UNIX_SECONDS_INTEGER_DATETIME_PARSING) \
                as transformer:

                transformed_record = transformer.transform(
                    record,
                    schema,
                    stream_metadata)

                # Reset max_bookmark_value to new value if higher
                if bookmark_field and (bookmark_field in transformed_record):

                    prev_max_bookmark_value = transform_datetime(max_bookmark_value)

                    if prev_max_bookmark_value and (transformed_record[bookmark_field]):
                        if transformed_record[bookmark_field] > prev_max_bookmark_value:
                            max_bookmark_value = transformed_record[bookmark_field]
                            LOGGER.info('{}, increase max_bookmark_value: {}'.format(
                                stream_name,
                                max_bookmark_value))
                    else:
                        max_bookmark_value = transformed_record[bookmark_field]
                        LOGGER.info('{}, increase max_bookmark_value: {}'.format(
                            stream_name,
                            max_bookmark_value))

                write_record(stream_name,
                            transformed_record,
                            time_extracted=time_extracted)
                counter.increment()

        return max_bookmark_value, counter.value


# Sync a specific endpoint.
def sync_endpoint(client, #pylint: disable=too-many-branches
                  catalog,
                  state,
                  start_date,
                  stream_name,
                  path,
                  endpoint_config,
                  static_params,
                  bookmark_query_field=None,
                  bookmark_field=None,
                  bookmark_type=None,
                  data_key=None,
                  id_fields=None,
                  selected_streams=None,
                  parent=None,
                  parent_id=None):

    # Get the latest bookmark for the stream and set the last_integer/datetime
    last_datetime = None
    last_integer = None
    max_bookmark_value = None
    if bookmark_type == 'integer':
        last_integer = get_bookmark(state, stream_name, 0)
        max_bookmark_value = last_integer
    else:
        last_datetime = get_bookmark(state, stream_name, start_date)
        max_bookmark_value = last_datetime
        LOGGER.info('{}, initial max_bookmark_value {}'.format(stream_name, max_bookmark_value))
        # max_bookmark_dttm = strptime_to_utc(last_datetime)

    # Pagination: loop thru all pages of data using next_page (if not None)
    page = 1
    offset = 0
    total_records = 0

    # Adds in endpoint specific, sort, filter params
    params = {
        'page': page,
        **static_params
    }

    next_url = '{}/{}'.format(client.base_url, path)

    i = 1
    while params['page'] is not None:
        # Need URL querystring for 1st page; subsequent pages provided by next_page
        # querystring: Squash query params into string
        if i == 1:
            if bookmark_query_field:
                if bookmark_type == 'datetime':
                    params[bookmark_query_field] = last_datetime
                elif bookmark_type == 'integer':
                    params[bookmark_query_field] = last_integer

        if params != {}:
            querystring = '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])

        # API request data
        data = {}
        data = client.get(
            url=next_url,
            path=path,
            params=querystring,
            endpoint=stream_name)

        # time_extracted: datetime when the data was extracted from the API
        time_extracted = utils.now()

        if not data or data is None or data == {}:
            return total_records

        # Transform data with transform_json from transform.py
        # The data_key identifies the array/list of records below the <root> element.
        # SINGLE RECORD data results appear as dictionary.
        # MULTIPLE RECORD data results appear as an array-list under the data_key.
        # The following code converts ALL results to an array-list and transforms data.
        transformed_data = []
        data_list = []
        data_dict = {}

        transformed_data = transform_json(data, stream_name, data_key)

        # TODO: comment out if not debugging
        # LOGGER.info('transformed_data = {}'.format(transformed_data))

        # No data returned
        if not transformed_data or transformed_data is None:
            if parent_id is None:
                LOGGER.info('Stream: {}, No transformed data for data = {}'.format(
                    stream_name, data))
            return total_records

        # Verify key id_fields are present
        rec_count = 0
        for record in transformed_data:
            for key in id_fields:
                if not record.get(key):
                    LOGGER.info('Stream: {}, Missing key {} in record: {}'.format(
                        stream_name, key, record))
                    raise RuntimeError
            rec_count = rec_count + 1

        # Process records and get the max_bookmark_value and record_count for the set of records
        max_bookmark_value, record_count = process_records(
            catalog=catalog,
            stream_name=stream_name,
            records=transformed_data,
            time_extracted=time_extracted,
            bookmark_field=bookmark_field,
            bookmark_type=bookmark_type,
            max_bookmark_value=max_bookmark_value,
            last_datetime=last_datetime,
            last_integer=last_integer,
            parent=parent,
            parent_id=parent_id)

        # set total_records and next_url for pagination
        total_records = total_records + record_count

        def parse_page_number(next_page_string):
            if next_page_string:
                return next_page_string.split('=')[-1]
            return next_page_string

        next_page_query_string = data.get('next_page', None)
        params['page'] = parse_page_number(next_page_query_string)

        # Update the state with the max_bookmark_value
        if bookmark_field:
            write_bookmark(state, stream_name, max_bookmark_value)

        # to_rec: to record; ending record for the batch page
        to_rec = offset + rec_count
        LOGGER.info('Synced Stream: {}, page: {}, records: {} to {}'.format(
            stream_name,
            page,
            offset,
            to_rec))
        # Pagination: increment the offset by the limit (batch-size) and page
        offset = offset + rec_count
        page = page + 1
        i = i + 1

    # Return total_records across all pages
    LOGGER.info('Synced Stream: {}, pages: {}, total records: {}'.format(
        stream_name,
        page - 1,
        total_records))

    # Update the state with the max_bookmark_value for non-scrolling
    if bookmark_field:
        write_bookmark(state, stream_name, max_bookmark_value)

    return total_records


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


# List selected fields from stream catalog
def get_selected_fields(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    mdata = metadata.to_map(stream.metadata)
    mdata_list = singer.metadata.to_list(mdata)
    selected_fields = []
    for entry in mdata_list:
        field = None
        try:
            field = entry['breadcrumb'][1]
            if entry.get('metadata', {}).get('selected', False):
                selected_fields.append(field)
        except IndexError:
            pass
    return selected_fields

def sync(client, config, catalog, state):
    if 'start_date' in config:
        start_date = config['start_date']

    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)

    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)

    if not selected_streams:
        return

    # Loop through selected_streams
    for stream_name, endpoint_config in STREAMS.items():
        if stream_name in selected_streams:

            LOGGER.info('Start Syncing: {}'.format(stream_name))

            selected_fields = get_selected_fields(catalog, stream_name)

            update_currently_syncing(state, stream_name)

            path = endpoint_config.get('path', stream_name)

            bookmark_field = next(iter(endpoint_config.get('replication_keys', [])), None)

            write_schema(catalog, stream_name)

            total_records = sync_endpoint(
                client=client,
                catalog=catalog,
                state=state,
                start_date=start_date,
                stream_name=stream_name,
                path=path,
                endpoint_config=endpoint_config,
                static_params=endpoint_config.get('params', {}),
                bookmark_query_field=endpoint_config.get('bookmark_query_field', None),
                bookmark_field=bookmark_field,
                bookmark_type=endpoint_config.get('bookmark_type', None),
                data_key=endpoint_config.get('data_key', stream_name),
                id_fields=endpoint_config.get('key_properties'),
                selected_streams=selected_streams)

            update_currently_syncing(state, None)
            LOGGER.info('FINISHED Syncing: {}, total_records: {}'.format(
                stream_name,
                total_records))
