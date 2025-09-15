import collections
import datetime
import enum
import json
import logging
import math
import os
import random
import string
import sys
import uuid
from collections.abc import Mapping
from typing import NamedTuple

import dateutil
import jwt
from argon2 import PasswordHasher, Type
from argon2.profiles import RFC_9106_LOW_MEMORY

ph = PasswordHasher(
    type=Type.ID,
    time_cost=3,
    memory_cost=64 * 1024,
    parallelism=4,
    salt_len=16,
    hash_len=32,
)


class UserToken(NamedTuple):
    jti: str
    exp: datetime.datetime
    token: str


class LoggerAware:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger = get_logger(self.__class__.__name__)


def random_id(length: int) -> str:
    return "".join(
        random.choices(string.ascii_lowercase + string.digits, k=length)
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"app.{name}")


def intval(val: str) -> int:
    try:
        return int("".join([n for n in val if n.isdigit()]))
    except:
        return 0


def floatval(val: str) -> float:
    try:
        return float(val)
    except:
        return 0


def dt2str(dt, format="%Y-%m-%d %H:%M:%S"):
    if isinstance(dt, datetime.datetime):
        return dt.strftime(format)
    return dt


def str2dt(date_time_str, format="%Y-%m-%d %H:%M:%S"):
    return datetime.datetime.strptime(date_time_str, format).replace(
        tzinfo=datetime.timezone.utc
    )


def dt2ut(date_time):
    if isinstance(date_time, datetime.datetime):
        return int(date_time.timestamp())
    return date_time


def get_env(key, placeholder=None):
    value = os.getenv(key, placeholder)
    if value is None:
        return placeholder

    if len(str(value).strip()) == 0:
        return placeholder

    return value


def get_docker_secret(path, placeholder=None, path_from_env=False):
    if path_from_env:
        path = get_env(path, None)

    if path is None:
        return placeholder

    if not os.path.exists(path):
        return placeholder

    try:
        with open(path, "r") as f:
            return f.read().strip()
    except:
        return placeholder


def str_or_none(value):
    if value is None:
        return None
    elif len(str(value).strip()) == 0:
        return None
    return value


class Map(dict):
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)
        for arg in args:
            if isinstance(arg, dict):
                for k, v in arg.items():
                    self[k] = v

        if kwargs:
            for k, v in kwargs.items():
                self[k] = v

    """
    Intercept attribute for better exception message
    def __getitem__(self, key):
        val = dict.__getitem__(self, key)
        return val
    """

    def __getattr__(self, attr):
        return self.get(attr)

    def __setattr__(self, key, value):
        self.__setitem__(key, value)

    def __setitem__(self, key, value):
        super(Map, self).__setitem__(key, value)
        self.__dict__.update({key: value})

    def __delattr__(self, item):
        self.__delitem__(item)

    def __delitem__(self, key):
        super(Map, self).__delitem__(key)
        del self.__dict__[key]


def boolval(value) -> bool:
    if value == "true":
        return True
    elif value == "True":
        return True
    elif value == "false":
        return False
    elif value == "False":
        return False
    elif value == "0":
        return False
    elif value == 0:
        return False
    elif value == "1":
        return True
    elif value == 1:
        return True
    else:
        return False


class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif isinstance(obj, datetime.timedelta):
            return (datetime.datetime.min + obj).time().isoformat()

        # UUID
        elif isinstance(obj, uuid.UUID):
            return str(obj)

        # Enum
        elif isinstance(obj, enum.Enum):
            return obj.value

        return super().default(obj)


def json_dumps(data):
    return json.dumps(data, cls=CustomJsonEncoder)


def hmac_hashing(secret, payload):
    m = hmac.new(
        secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256
    )
    return m.hexdigest()


def rsa_signature(private_key, payload, private_key_pass=None):
    private_key = RSA.import_key(private_key, passphrase=private_key_pass)
    h = SHA256.new(payload.encode("utf-8"))
    signature = pkcs1_15.new(private_key).sign(h)
    return b64encode(signature)


def dict_merge(d, u) -> dict:
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = dict_merge(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def create_jwt_token(data, secret, expiry, iss="app"):
    """Generate a JWT token with expiration"""
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    jti = str(uuid.uuid4())
    exp = now + expiry
    payload = {
        **data,
        "jti": jti,
        "iss": iss,
        "iat": now,
        "nbf": now,
        "exp": exp,
    }
    return UserToken(
        jti=jti, exp=exp, token=jwt.encode(payload, secret, algorithm="HS256")
    )


def verify_jwt_token(token, secret):
    """Verify a JWT token"""
    try:
        payload = jwt.decode(token, secret, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        return "expired"
    except jwt.InvalidTokenError:
        return None


def normalize_csv_number(value, limit):
    if not value:
        return [1] * limit

    value = str(value)
    numbers = [float(x.strip()) for x in value.split(",") if x.strip()]

    if len(numbers) > limit:
        return numbers[:limit]

    if len(numbers) < limit:
        numbers.extend([numbers[-1]] * (limit - len(numbers)))

    return numbers


def list_get(obj, index, default=None):
    try:
        return obj[index]
    except IndexError:
        return default


def get_attr(obj, path, default=None):
    for part in path.split("."):
        if obj is None:
            return default
        if isinstance(obj, Mapping):
            obj = obj.get(part, default)
        else:
            obj = getattr(obj, part, None)
    return obj if obj is not None else default


def has_attr(obj, path):
    for part in path.split("."):
        if obj is None:
            return False
        if isinstance(obj, dict):
            if part not in obj:
                return False
            obj = obj[part]
        else:
            if not hasattr(obj, part):
                return False
            obj = getattr(obj, part)
    return obj is not None


def set_attr(obj, path, value):
    parts = path.split(".")
    for part in parts[:-1]:
        if isinstance(obj, dict):
            if part not in obj or obj[part] is None:
                obj[part] = {}
            obj = obj[part]
        else:
            if not hasattr(obj, part) or getattr(obj, part) is None:
                raise AttributeError(
                    f"Cannot auto-create '{part}' on object of type {type(obj)}"
                )
            obj = getattr(obj, part)

    last = parts[-1]
    if isinstance(obj, dict):
        obj[last] = value
    else:
        setattr(obj, last, value)


def dict_append(obj, key, value, value_as_list=False):
    if key not in obj:
        obj[key] = [] if value_as_list else value
    if value_as_list:
        obj[key].append(value)
    else:
        obj[key] = value


def upsert_item(obj_list, key, match_value, extra_data=None, replace=False):
    """
    Upserts an item into a list of dicts.

    - Searches for an object where obj[key] == match_value.
    - If found:
        - Replaces it with {key: match_value, **extra_data} if replace=True.
        - Else, does nothing.
    - If not found:
        - Appends {key: match_value, **extra_data}.

    Parameters:
        obj_list (list): List of dicts.
        key (str): Key to match on.
        match_value (any): Value to match.
        extra_data (dict): Additional fields to include.
        replace (bool): Whether to replace existing object if found.

    Returns:
        list: The updated list.
    """
    if extra_data is None:
        extra_data = {}

    new_obj = {key: match_value, **extra_data}

    for i, obj in enumerate(obj_list):
        if obj.get(key) == match_value:
            if replace:
                obj_list[i] = new_obj
            return obj_list

    obj_list.append(new_obj)
    return obj_list


def eval_js(js_code):
    try:
        return pm.eval(js_code, {"strict": True, "module": False})
    except:
        print([js_code, sys.exc_info()])
        return json.dumps({"error": True})


def ceil_to_step(value, step=0.001):
    result = math.ceil(value / step) * step
    # Fix the number of decimals based on step (e.g., 0.01 -> 2 decimals)
    decimals = max(0, -int(math.log10(step)))
    return round(result, decimals)


def count_decimals(value):
    # Convert to string using full precision
    value_str = (
        format(value, ".16e") if isinstance(value, float) else str(value)
    )

    if "." not in value_str and "e" not in value_str:
        return 0  # It's an integer

    if "e-" in value_str:
        # Scientific notation like 1.2e-5
        base, exponent = value_str.split("e-")
        base_decimals = (
            len(base.split(".")[-1].rstrip("0")) if "." in base else 0
        )
        return int(exponent) + base_decimals
    elif "." in value_str:
        return len(value_str.split(".")[-1].rstrip("0"))

    return 0


def normalize_datetime(value) -> datetime.datetime:
    if value is None:
        return value

    if isinstance(value, int):
        # Assuming milliseconds since epoch
        dt = datetime.datetime.utcfromtimestamp(value / 1000)
    elif isinstance(value, str):
        try:
            dt = dateutil.parser.parse(value)
        except Exception:
            raise ValueError(f"Unrecognized datetime string: {value}")
    elif isinstance(value, datetime.datetime):
        dt = value
    else:
        raise ValueError("Invalid datetime format.")

    # Force timezone to UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    else:
        dt = dt.astimezone(datetime.timezone.utc)

    return dt


def normalize_json(value):
    if value is None:
        return None
    if isinstance(value, str):  # Convert JSON string to dict
        return json.loads(value)
    if isinstance(value, dict):  # Already a dict
        return value
    if isinstance(value, list):
        return value
    raise ValueError("data must be a JSON string, dict, list or None")
