import base64
import hashlib
import typing
from datetime import datetime, timezone
from decimal import Decimal, Context

DECIMAL_CTX = Context(prec=40)


def cast_as_int_or_float(value):
    if float(value).is_integer():
        return int(value)
    else:
        return float(value)
    

def cast_as_int_or_decimal(value):
    decimal_value = Decimal(value).normalize(context=DECIMAL_CTX)
    int_value = int(decimal_value)
    if int_value == decimal_value:
        return int_value
    else:
        return decimal_value


def normalize_data_type(value):
    if value is None:
        return value
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(value).decode('utf-8')
    
    if isinstance(value, int):
        return int(value)
    elif isinstance(value, Decimal):
        return cast_as_int_or_decimal(value)
    elif isinstance(value, float):
        return cast_as_int_or_float(value)
    elif str(value).replace('.','',1).isnumeric():  #handles cases like '1.0000' and 1 or '1.1' and 1.1
        return cast_as_int_or_decimal(value)
    elif isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).timestamp()
    else:
        ValueError(f"Unexpected type {type(value)}")


def generate_surrogate_key(data: typing.Dict[str, typing.Any], keys: typing.List[str]):
    values = []
    for key in keys:
        if data[key] is None:
            values.append('_._--NULL--_._')
        elif isinstance(data[key], str):
            values.append(data[key])
        else:
            values.append(str(normalize_data_type(data[key])))
    
    return hashlib.md5("#$%^#".join(values).encode('utf-8')).hexdigest()
