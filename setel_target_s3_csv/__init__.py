#!/usr/bin/env python3
import target_s3_csv
import singer
import pytz
from target_s3_csv import *
from setel_target_s3_csv.s3 import upload_file_with_tagset
import simplejson


class OneOneMessage(singer.messages.RecordMessage):
    def __init__(self, stream, record, TagSet, _sdc_source_file, sync_one_one=True, version=None, time_extracted=None):
        self.stream = stream
        self.record = record
        self.version = version
        self.time_extracted = time_extracted
        self.TagSet = TagSet
        self.sync_one_one = sync_one_one
        self._sdc_source_file = _sdc_source_file
        if time_extracted and not time_extracted.tzinfo:
            raise ValueError("'time_extracted' must be either None " +
                             "or an aware datetime (with a time zone)")

    def asdict(self):
        result = {
            'type': 'ONE_ONE_RECORD',
            'stream': self.stream,
            'record': self.record,
            'TagSet': self.TagSet,
            'sync_one_one': self.sync_one_one,
            '_sdc_source_file': self._sdc_source_file,
        }
        if self.version is not None:
            result['version'] = self.version
        if self.time_extracted:
            as_utc = self.time_extracted.astimezone(pytz.utc)
            result['time_extracted'] = singer.utils.strftime(as_utc)
        return result


def parse_one_one_message(msg):
    obj = simplejson.loads(msg, use_decimal=True)
    msg_type = singer.messages._required_key(obj, 'type')

    if msg_type == 'ONE_ONE_RECORD':
        time_extracted = obj.get('time_extracted')
        if time_extracted:
            try:
                time_extracted = singer.messages.ciso8601.parse_datetime(time_extracted)
            except:
                singer.messages.LOGGER.warning("unable to parse time_extracted with ciso8601 library")
                time_extracted = None

            # time_extracted = dateutil.parser.parse(time_extracted)
        return OneOneMessage(stream=singer.messages._required_key(obj, 'stream'),
                             record=singer.messages._required_key(obj, 'record'),
                             TagSet=obj.get("TagSet"),
                             _sdc_source_file=obj.get("_sdc_source_file"),
                             sync_one_one=obj.get("sync_one_one"),
                             version=obj.get('version'),
                             time_extracted=time_extracted)


def persist_messages(messages, config, s3_client):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}
    TagSet = {}
    delimiter = config.get('delimiter', ',')
    quotechar = config.get('quotechar', '"')

    # Use the system specific temp directory if no custom temp_dir provided
    temp_dir = os.path.expanduser(config.get('temp_dir', tempfile.gettempdir()))

    # Create temp_dir if not exists
    if temp_dir:
        os.makedirs(temp_dir, exist_ok=True)

    filenames = []
    now = datetime.now().strftime('%Y%m%dT%H%M%S')
    sync_one_one = False
    for message in messages:
        try:
            o = singer.parse_message(message)
            if o is None:
                o = parse_one_one_message(message)
            o = o.asdict()
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(message))
            raise
        message_type = o['type']
        if message_type == 'RECORD' or message_type == 'ONE_ONE_RECORD':
            if o['stream'] not in schemas:
                raise Exception("A record for stream {}"
                                "was encountered before a corresponding schema".format(o['stream']))

            # Validate record
            try:
                validators[o['stream']].validate(utils.float_to_decimal(o['record']))
            except Exception as ex:
                if type(ex).__name__ == "InvalidOperation":
                    logger.error("Data validation failed and cannot load to destination. RECORD: {}\n"
                                 "'multipleOf' validations that allows long precisions are not supported"
                                 " (i.e. with 15 digits or more). Try removing 'multipleOf' methods from JSON schema."
                                 .format(o['record']))
                    raise ex

            record_to_load = o['record']
            if config.get('add_metadata_columns'):
                record_to_load = utils.add_metadata_values_to_record(o, {})
            else:
                record_to_load = utils.remove_metadata_values_from_record(o)

            sync_one_one = o.get("sync_one_one", False)

            if not sync_one_one:
                filename = o['stream'] + '-' + now + '.csv'
                filename = os.path.expanduser(os.path.join(temp_dir, filename))
                target_key = utils.get_target_key(o,
                                                  prefix=config.get('s3_key_prefix', ''),
                                                  timestamp=now,
                                                  naming_convention=config.get('naming_convention'))
            else:
                prefix = config.get('s3_key_prefix', '')
                _sdc_source_file = os.path.join(prefix,o['_sdc_source_file'])
                target_key = os.path.join(_sdc_source_file)
                filename = os.path.expanduser(os.path.join(temp_dir, target_key))
                if filename not in TagSet.keys():
                    TagSet[filename] = o['TagSet']
            if not (filename, target_key) in filenames:
                filenames.append((filename, target_key))

            file_is_empty = (not os.path.isfile(filename)) or os.stat(filename).st_size == 0

            flattened_record = utils.flatten_record(record_to_load)
            if sync_one_one:
                header_key = target_key
            else:
                header_key = o['stream']
            if header_key not in headers and not file_is_empty:
                with open(filename, 'r') as csvfile:
                    reader = csv.reader(csvfile,
                                        delimiter=delimiter,
                                        quotechar=quotechar)
                    first_line = next(reader)
                    headers[header_key] = first_line if first_line else flattened_record.keys()
            else:
                headers[header_key] = flattened_record.keys()
            if not os.path.exists(os.path.dirname(filename)):
                os.makedirs(os.path.dirname(filename))
            with open(filename, 'a') as csvfile:
                writer = csv.DictWriter(csvfile,
                                        headers[header_key],
                                        extrasaction='ignore',
                                        delimiter=delimiter,
                                        quotechar=quotechar)
                if file_is_empty:
                    writer.writeheader()

                writer.writerow(flattened_record)

            state = None
        elif message_type == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif message_type == 'SCHEMA':
            stream = o['stream']
            schemas[stream] = o['schema']
            if config.get('add_metadata_columns'):
                schemas[stream] = utils.add_metadata_columns_to_schema(o)

            schema = utils.float_to_decimal(o['schema'])
            validators[stream] = Draft7Validator(schema, format_checker=FormatChecker())
            key_properties[stream] = o['key_properties']
        elif message_type == 'ACTIVATE_VERSION':
            logger.debug('ACTIVATE_VERSION message')
        else:
            logger.warning("Unknown message type {} in message {}"
                           .format(o['type'], o))

    # Upload created CSV files to S3
    for filename, target_key in filenames:
        compressed_file = None
        if config.get("compression") is None or config["compression"].lower() == "none":
            pass  # no compression
        else:
            if config["compression"] == "gzip":
                compressed_file = f"{filename}.gz"
                with open(filename, 'rb') as f_in:
                    with gzip.open(compressed_file, 'wb') as f_out:
                        logger.info(f"Compressing file as '{compressed_file}'")
                        shutil.copyfileobj(f_in, f_out)
            else:
                raise NotImplementedError(
                    "Compression type '{}' is not supported. "
                    "Expected: 'none' or 'gzip'"
                        .format(config["compression"])
                )
        if sync_one_one:
            upload_file_with_tagset(compressed_file or filename,
                                    s3_client,
                                    config.get('s3_bucket'),
                                    target_key,
                                    encryption_type=config.get('encryption_type'),
                                    encryption_key=config.get('encryption_key'),
                                    tagset=TagSet[compressed_file or filename])
        else:
            s3.upload_file(compressed_file or filename,
                           s3_client,
                           config.get('s3_bucket'),
                           target_key,
                           encryption_type=config.get('encryption_type'),
                           encryption_key=config.get('encryption_key'))

        # Remove the local file(s)
        os.remove(filename)
        if compressed_file:
            os.remove(compressed_file)

    return state


target_s3_csv.persist_messages = persist_messages

if __name__ == '__main__':
    main()
