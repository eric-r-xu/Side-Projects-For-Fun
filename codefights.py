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
