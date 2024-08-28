# #n=int(input().strip())
# #if ((n+1)/2)%2==0:
# #    print (" gg boi")
# #else:
# #    print( "not gg")
#
# #import array
#
# arr = list(map(int, input().split()))
# arr.sort()
# print (arr)
# print(arr[-2])


# to interchange character case, long method
# listing = str(input())
# check_list = list(listing)
# print(check_list)
# final_list = []
# for x in check_list:
#     if x.isalpha():
#         if x.islower():
#             final_list.append(x.upper())
#         elif x.isupper():
#             final_list.append(x.lower())
#     else:
#         final_list.append(x)
# print(final_list)
# v = "".join(y for y in final_list)
# print(v)


# now to optimise this method we can use list comprehension
# new_list = "".join([x.upper() if x.islower() else x.lower() if x.isupper() else x for x in str(input())])
# print(new_list)


# splitting the string and adding hypen in between, long method
# f_list = []
#
#
# def newfunc(s):
#     # return "-".join(x if x.isaplha() else x for x in s)
#     new_s = list(s)
#     for x in new_s:
#         if x.isalpha():
#             f_list.append(x)
#         else:
#             f_list.append("-")
#     v = "".join(y for y in f_list)
#     print(v)
#
# newfunc("Aryan Vatsal")

# #short method
# def split_and_join(s):
#     return "".join([x if x.isalpha() else "-" for x in s])
#
# if __name__ == '__main__':
#     line = input()
#     result = split_and_join(line)
#     print(result)


# Important, list immuatation question
#
# def mutate_string(string, position, character):
#     return "".join([c if i != position - 1 else character for i, c in enumerate(string)])
#
#
# if __name__ == '__main__':
#     s = input()
#     i, c = input().split()
#     s_new = mutate_string(s, int(i), c)
#     print(s_new)
#


# textwrap function
# short wala method
# import textwrap
#
#
# def wrap(u, v):
#     wrapped_lines = textwrap.wrap(u, v)
#     return "\n".join(wrapped_lines)
#
#
# if __name__ == '__main__':
#     string, max_width = input(), int(input())
#     result = wrap(string, max_width)
#     print(result)


##brute force method

def func_for_wrapping(s, t):
    fresult = ""
    buffer_line = ""

    for i in s:
        buffer_line += i

        if len(buffer_line) == t:
            fresult += buffer_line + "\n"
            buffer_line = ""
    if buffer_line:
        fresult += buffer_line

    print(fresult)

func_for_wrapping("aryanvatsalsrivastava", 3)
