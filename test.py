import pytest
from SolutionUpload2 import SolutionUpload2


def test_empty():
    """Checks for empty input folder if it is"""
    instance = SolutionUpload2('/Users/vivekchaubey/Desktop/check', 'Combined.csv')
    instance.extract()
    val = len(list(instance.input_dataframes.keys()))
    assert val != 0


def test_source_ip_check():
    """Checking if "Source IP" column is in file or not"""
    instance = SolutionUpload2('/Users/vivekchaubey/Desktop/check', 'Combined.csv')
    instance.extract()
    if len(list(instance.input_dataframes.keys())) == 0:
        available = 0
    else:
        available = 1
    for file_name in instance.input_dataframes:
        if "Source IP" not in list(instance.input_dataframes[file_name].columns):
            available = -1
            break
    assert available == 1


def test_ip_check():
    """checks for ip address format"""
    instance = SolutionUpload2('/Users/vivekchaubey/Desktop/check', 'Combined.csv')
    instance.extract()

    if len(list(instance.input_dataframes.keys())) > 0:

        def valid_ip_format(input_ip):
            bool_ip = 1
            text = input_ip.split('.')

            rule = (0 < int(text[0]) < 256) and \
                   (0 <= int(text[1]) < 256) and \
                   (0 <= int(text[2]) < 256) and \
                   (0 <= int(text[3]) < 256)
            if len(text) != 4:
                bool_ip = 0
            elif rule:
                bool_ip = 1
            else:
                bool_ip = 0

            assert bool_ip == 1

        for each_file in instance.input_dataframes.keys():
            instance.input_dataframes[each_file]['Source IP'].apply(lambda x: valid_ip_format(x))
    else:
        assert 1 == 2
