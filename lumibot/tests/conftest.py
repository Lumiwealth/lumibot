import pytest

# def pytest_addoption(parser):
#     parser.addoption("--name", action="store", default="default name")
#
#
# def pytest_generate_tests(metafunc):
#     # This is called for every test. Only get/set command line arguments
#     # if the argument is specified in the list of test "fixturenames".
#     option_value = metafunc.config.option.name
#     # if 'name' in metafunc.fixturenames and \
#     if option_value is not None:
#         metafunc.parametrize("name", [option_value])


option = None


def pytest_addoption(parser):
    parser.addoption("--name", action="store", default="example",
                     help="Option for making some stuff")


def pytest_configure(config):
    global option
    option = config.option
