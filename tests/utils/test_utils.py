from python_databricks_utils.utils.utils import flatten_list


def test_flatten_list_depth0():
    assert flatten_list([1, 2]) == [1, 2]


def test_flatten_list_depth1():
    assert flatten_list([[1], 2]) == [1, 2]


def test_flatten_list_depth2():
    assert flatten_list([[[1]], 2]) == [1, 2]


def test_flatten_list_depth3():
    assert flatten_list([[[[1]]], 2]) == [1, 2]
