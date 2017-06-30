import datetime

MAX_TABLES_PER_PIPELINE = 32
READ_BLOCK_SIZE_BYTES = 4096
MAX_ALLOWED_PROVISIONED_READ_THROUGHPUT = 1000
MAX_MSG_BULK_READ = 100
MAX_BULK_READ_SIZE_BYTES = 16777216
MAX_DURATION_SEC = 14400
MAX_DURATION_SINGLE_PIPELINE = 3300  #less than 1h
ACTIVITY_BOOTSTRAP_TIME = 60
EMR_BOOTSTRAP_TIME = 600
INITIAL_READ_THROUGHPUT_PERCENT = 0.5
TIME_IN_BETWEEN_BACKUPS = 86400


def estimate_backup_duration(read_throughput_percent, table_size_bytes, read_capacity_units):
    """
    Gives rough estimate, on how long backing up dynamo db table will take.
    :param table_size_bytes:
    :param read_capacity_units
    :return: Estimated time in seconds.
    """
    read_bytes_per_second = read_capacity_units * read_throughput_percent * READ_BLOCK_SIZE_BYTES

    return table_size_bytes / read_bytes_per_second


def compute_required_throughput(estimated_duration, target_duration, read_capacity_units, read_throughput_percent):
    """
    :param estimated_duration: estimated duration using current: read_capacity_units, read_throughput_percent
    :param target_duration: how long should backup take
    :param read_capacity_units: current provisioned read capacity
    :param read_throughput_percent: current backup read throughput as % of total read throughput
    :return: new read throughput with new read throughput percent
    """
    ratio = estimated_duration / float(target_duration)
    new_read_capacity_units = read_capacity_units * (ratio + 1)
    new_read_throughput_percent = float(read_capacity_units) / new_read_capacity_units

    new_read_throughput_percent = 1 - max(new_read_throughput_percent, 0.01)

    return int(round(new_read_capacity_units)), round(new_read_throughput_percent, 2)


def get_date_suffix():
    return datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def list_tables_in_definition(pipeline_definition):
    nodes = pipeline_definition.get('objects')
    table_nodes = filter(lambda x: 'tableName' in x, nodes)

    return map(lambda x: x['tableName'], table_nodes)


def get_first_element_in_the_list_with(l, key, value):
    element = filter(lambda x: x[key] == value, l)

    if element:
        return element[0]

    return None

