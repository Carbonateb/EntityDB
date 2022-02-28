import pickle


def serialize(value: object) -> tuple[any, str]:
    '''Returns the final object and its mime type in a tuple'''
    t = type(value)

    if t is str:
        return value, "text/plain"

    elif t is bytes:
        return value, "application/octet-stream"

    elif t is int:
        return str(value), "text/plain"

    # No special overrides found, use pickle
    return pickle.dumps(value), "application/octet-stream"


def deserialize(value: bytes, out_type: type) -> object:

    if out_type is str:
        return value.decode()

    elif out_type is bytes:
        return value

    elif out_type is int:
        return int(value)

    # No special overrides found, use pickle
    return pickle.loads(value)
