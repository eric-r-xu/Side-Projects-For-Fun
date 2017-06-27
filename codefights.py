################# ARRAYS #################
# Given an array a that contains only numbers in the range from 1 to a.length, find the first duplicate number for which the 
# second occurrence has the minimal index. In other words, if there are more than 1 duplicated numbers, return the number 
# for which the second occurrence has a smaller index than the second occurrence of the other number does. 
# If there are no such elements, return -1.
def firstDuplicate(a):
  if len(a)==len(set(a)):
    return -1
  else:
    order = []
    counts = {}
    for x in a:
      if x in counts:
        order.append(x)
      else:
        counts[x] = 1
      if len(order)>0:
        return order[0]
    
      
# Given a string s, find and return the first instance of a non-repeating character in it. 
# If there is no such character, return '_'.     
def firstNotRepeatingCharacter(s):
  dict_var = {}
  order_var = []
  for each in s:
    if each in dict_var:
      dict_var[each] += 1
    else:
      dict_var[each] = 1
      order_var.append(each)
  for each in order_var:
    if dict_var[each] == 1:
      return each
  return '_'

# You are given an n x n 2D matrix that represents an image. Rotate the image by 90 degrees (clockwise).
def rotateImage(a):
    # assume len and width the same
    len_a = len(a)
    recreated_Image = []
    for i in range(0, len_a):
        temp = []
        for j in range(1, len_a+1):
            temp.append(a[len_a - j][i])
        recreated_Image.append(temp)  
    return recreated_Image

  
# Sudoku is a number-placement puzzle. The objective is to fill a 9 × 9 grid with numbers in such a way that each column, 
# each row, and each of the nine 3 × 3 sub-grids that compose the grid all contain all of the numbers from 1 to 9 one time.
# Implement an algorithm that will check whether the given grid of numbers represents a valid Sudoku puzzle according to the 
# layout rules described above. 
# Note that the puzzle represented by grid does not have to be solvable.
  def sudoku2(grid):
    valid_chars = ['1','2','3','4','5','6','7','8','9']
    for i in range(0,9):
        horizontal_dict={}
        vertical_dict={}
        three_by_three_dict = {}
        for j in range(0,9):
            translated_i = ((i % 3)*3) + ((j / 3))
            translated_j = (j % 3) + ((i / 3)*3)
            for each in grid[i][j]:
                if each in valid_chars:
                    if each in horizontal_dict:
                        # duplicate found!
                        print 'failed horizontal_dict'
                        return False
                    else:
                        horizontal_dict[each] = 1   
            for each in grid[j][i]:
                if each in valid_chars:
                    if each in vertical_dict:
                        # duplicate found!
                        print 'failed vertical_dict'
                        return False
                    else:
                        vertical_dict[each] = 1       
            for each in grid[translated_i][translated_j]:
                if each in valid_chars:
                    if each in three_by_three_dict:
                        # duplicate found!
                        print 'failed three_by_three_dict'
                        return False
                    else:
                        three_by_three_dict[each] = 1
    return True

  
# A cryptarithm is a mathematical puzzle for which the goal is to find the correspondence between letters and digits, 
# such that the given arithmetic equation consisting of letters holds true when the letters are converted to digits.
# You have an array of strings crypt, the cryptarithm, and an an array containing the mapping of letters and digits, solution. 
# The array crypt will contain three non-empty strings that follow the structure: [word1, word2, word3], which should be 
# interpreted as the word1 + word2 = word3 cryptarithm.
# If crypt, when it is decoded by replacing all of the letters in the cryptarithm with digits using the mapping in solution, 
# becomes a valid arithmetic equation containing no numbers with leading zeroes, the answer is true. 
# If it does not become a valid arithmetic solution, the answer is false.  
def isCryptSolution(crypt, solution):
    solution_dict = {}
    for each in solution:
        solution_dict[each[0]] = each[1]
        
    first_sum_num = ''
    second_sum_num = ''
    sum_num = ''
    for each in crypt[0]:
        first_sum_num = ''.join([first_sum_num, solution_dict[each]])
    for each in crypt[1]:
        second_sum_num = ''.join([second_sum_num, solution_dict[each]])
    for each in crypt[2]:
        sum_num = ''.join([sum_num, solution_dict[each]])
    
    if (len(first_sum_num) > 1 and first_sum_num[0] == '0'):
        return False
    elif (second_sum_num[0] == '0' and  len(second_sum_num) > 1):
        return False
    elif sum_num[0] == '0' and len(sum_num) > 1:
        return False
    else:
        if int(first_sum_num) + int(second_sum_num) == int(sum_num):
            return True
        else:
            return False
################# LINKED LISTS #################
# Definition for singly-linked list:
# class ListNode(object):
#   def __init__(self, x):
#     self.value = x
#     self.next = None

# Given a singly linked list of integers l and a non-negative integer k, remove all elements from 
# list l that have a value equal to k.
def removeKFromList(l,k):
	# iterating through linked list
	a = []
	while True:
		if l == None:
			break
		if l.value != k:
			a.append(l.value)
		l = l.next

	t = ListNode(None)
	r = ListNode(None)

	if len(a)>0:
		for i in (reversed(range(0, len(a)))):
			r = t
			t = ListNode(a[i])
			if i < len(a) - 1:
				t.next = r
		return t
	else:
		return []

# Given a singly linked list of integers, determine whether or not it's a palindrome.
def isListPalindrome(l):
    # iterating through linked list
    a = []
    while True:
        if l == None:
            break
        a.append(l.value)
        l = l.next
    if list(reversed(a))[0:len(a)/2] == a[0:len(a)/2]:
        return True
    else:
        return False

# You're given 2 huge integers represented by linked lists. Each linked list element is a number from 0 to 9999 
# that represents a number with exactly 4 digits. The represented number might have leading zeros. 
# Your task is to add up these huge integers and return the result in the same format.
def addTwoHugeNumbers(a, b):
    # iterating through linked list
    a_str = ''
    b_str = ''
    while True:
        if a == None:
            break
        a_str = ''.join([a_str, str(a.value).zfill(4)])
        a = a.next
    while True:
        if b == None:
            break
        b_str = ''.join([b_str, str(b.value).zfill(4)])
        b = b.next
    output = int(a_str) + int(b_str)
    zz = len(str(output))
    output = str(output).zfill(4*(((zz-1)/4)+1))
    t = ListNode(None)
    r = ListNode(None)
    ii = ((zz-1)/4) + 1
    for i in reversed(range(0,ii)):
        r = t
        t = ListNode(int(output[(i*4):(i+1)*4]))
        print int(output[(i*4):(i+1)*4])
        if i < (ii)-1:
            t.next = r
    return t

# Given two singly linked lists sorted in non-decreasing order, your task is to merge them. 
# In other words, return a singly linked list, also sorted in non-decreasing order, that contains the 
# elements from both original lists.
def mergeTwoLinkedLists(l1, l2):
    new_list = list()
    counter = 0
    while True:
        if l1 == None:
            break
        new_list.append(l1.value)
        counter += 1
        l1 = l1.next
    while True:
        if l2 == None:
            break
        new_list.append(l2.value)
        l2 = l2.next
        counter += 1
    
    t = ListNode(None)
    r = ListNode(None)
    new_list = sorted(new_list)
    if counter == 0:
        return []
    else:
        for i in reversed(range(0,counter)):
            r = t
            t = ListNode(new_list[i])
            if i < counter - 1:
                t.next = r
        return t

# Given a linked list l, reverse its nodes k at a time and return the modified list. k is a positive integer that is 
# less than or equal to the length of l. If the number of nodes in the linked list is not a multiple of k, then the nodes 
# that are left out at the end should remain as-is.
# You may not alter the values in the nodes - only the nodes themselves can be changed.
def reverseNodesInKGroups(l, k):
    new_list = list()
    new_reversed_values = list()
    new_normal_values = list()
    counter = 0
    while True:
        if l == None:
            new_list.extend(new_normal_values)
            break
        if counter < k:
            new_reversed_values = [l.value] + new_reversed_values
            new_normal_values.append(l.value)
        l = l.next
        counter += 1
        if counter == k:
            counter = 0
            new_list.extend(new_reversed_values)
            new_reversed_values = list()
            new_normal_values = list()
    return new_list

# Given a singly linked list of integers l and a non-negative integer n, move the last n list nodes to the 
# beginning of the linked list.
def rearrangeLastN(l, n):
    new_list = list()
    while True:
        if l == None:
            break
        new_list.append(l.value)
        l = l.next
    return new_list[-n:] + new_list[0:-n]

################# HASH TABLES #################
# You have a list of dishes. Each dish is associated with a list of ingredients used to prepare it. You want to to 
# group the dishes by ingredients, so that for each ingredient you'll be able to find all the dishes that contain it 
# (if there are at least 2 such dishes).
# Return an array where each element is a list with the first element equal to the name of the ingredient and all of 
# the other elements equal to the names of dishes that contain this ingredient. The dishes inside each list should be 
# sorted lexicographically. The result array should be sorted lexicographically by the names of the ingredients in 
# its elements.
def groupingDishes(dishes):
    all_dishes = []
    all_ingredients = []
    popular_ingredients = set()
    new_dict = {}
    ingredient_dish_dict = {}
    for each in dishes:
        all_dishes.append(each[0])
        for each_ingredient in each[1:]:
            if each_ingredient in new_dict:
                popular_ingredients.add(each_ingredient)
            else:
                new_dict[each_ingredient] = 1

    popular_ingredients_list = list(popular_ingredients)
    
    new_array = []
    for each_pop_ingredient in sorted(popular_ingredients):
        temp = []
        temp2 = set()
        for each in dishes:
            if each_pop_ingredient in each[1:]:
                temp2.add(each[0])
        temp2_list = list(temp2)
        temp.append(each_pop_ingredient)
        temp.extend(sorted(temp2_list))
        new_array.append(temp)
    return new_array   

# Given an array strings, determine whether it follows the sequence given in the patterns array. In other words, 
# there should be no i and j for which strings[i] = strings[j] and patterns[i] ≠ patterns[j] or for which 
# strings[i] ≠ strings[j] and patterns[i] = patterns[j].
def areFollowingPatterns(strings, patterns):
    if len(strings) != len(patterns):
        return False
    else:
        new_dict_1 = {}
        new_dict_2 = {}
        strings_numbered = []
        patterns_numbered = []
        counter_1 = 1
        counter_2 = 1
        for ii in range(0, len(strings)):
            if strings[ii] in new_dict_1:
                strings_numbered.append(new_dict_1[strings[ii]])
            else:
                new_dict_1[strings[ii]] = counter_1
                strings_numbered.append(new_dict_1[strings[ii]])
                counter_1 += 1
            
            if patterns[ii] in new_dict_2:
                patterns_numbered.append(new_dict_2[patterns[ii]])
            else:
                new_dict_2[patterns[ii]] = counter_2
                patterns_numbered.append(new_dict_2[patterns[ii]])
                counter_2 += 1
        if strings_numbered == patterns_numbered:
            return True
        else:
            return False

# Given an array of integers nums and an integer k, determine whether there are two distinct indices i and j in the 
# array where nums[i] = nums[j] and the absolute difference between i and j is less than or equal to k.
def containsCloseNums(nums, k):
    new_dict = {}
    for i in range(0, len(nums)):
        if nums[i] in new_dict:
            if i - new_dict[nums[i]] <= k:
                return True
                break
            else:
                new_dict[nums[i]] = i
        else:
            new_dict[nums[i]] = i
    return False
