def flatten_list(l: list) -> list:
    flattened = []

    for element in l:
        if isinstance(element, list):
            flattened.extend(flatten_list(element))
        else:
            flattened.append(element)

    return flattened


def throw(ex: Exception = None):
    if ex is None:
        raise Exception
    else:
        raise Exception from ex


def unsafe[O](fn: Callable[..., O], success: O):
    result = fn()
    if result is not success:
        raise Exception(result)
