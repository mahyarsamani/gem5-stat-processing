import re


def extract_numerical_substrings(string):
    return re.findall(r"\d+", string)


def remove_numerical_characters(string):
    return re.sub(r"\d+", "", string)
