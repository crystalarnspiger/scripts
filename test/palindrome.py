import copy


def palindrome(word):
    new_word = ''
    x = len(word) - 1
    while x >= 0:
        new_word += word[x]
        x -= 1
    if word == new_word:
        return True
