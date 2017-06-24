# Given an array a that contains only numbers in the range from 1 to a.length, find the first duplicate number for which the second occurrence has the minimal index. In other words, if there are more than 1 duplicated numbers, return the number for which the second occurrence has a smaller index than the second occurrence of the other number does. If there are no such elements, return -1.
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
    
      
# Given a string s, find and return the first instance of a non-repeating character in it. If there is no such character, return '_'.     
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
