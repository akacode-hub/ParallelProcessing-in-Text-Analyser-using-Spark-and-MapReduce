def to_lower_case(s):
    """ Convert a string to lowercase. E.g., 'BaNaNa' becomes 'banana'. 
    """
    return s.lower()


def create_list_from_file(filename):
    """Read words from a file and convert them to a list.
   
    Input:
    - filename: The name of a file containing one word per line.
    
    Returns
    - wordlist: a list containing all the words in the file, as strings.

    """
    wordlist = []
    with open(filename) as f:
        line = f.readline()
        while line:
            wordlist.append(line.strip())
            line = f.readline()
        return wordlist     


def strip_non_alpha(s):
    """ Remove non-alphabetic characters from the beginning and end of a string. 

    E.g. ',1what?!"' should become 'what'. Non-alphabetic characters in the middle 
    of the string should not be removed. E.g. "haven't" should remain unaltered."""
    
    while len(s)>0 and not s[-1].isalpha():
         s=s[:-1]
    while len(s)>0 and not s[0].isalpha():
        s=s[1:]
    return s
    
    
    


def is_inflection_of(s1,s2):
    """ Tests if s1 is a common inflection of s2. 

    The function first (a) converts both strings to lowercase and (b) strips
    non-alphabetic characters from the beginning and end of each string. 
    Then, it returns True if the two resulting two strings are equal, or 
    the first string can be produced from the second by adding the following
    endings:
    (a) 's
    (b) s
    (c) es
    (d) ing
    (e) ed
    (f) d
    """

    s1=s1.lower()
    s2=s2.lower()
    s1=strip_non_alpha(s1)
    s2=strip_non_alpha(s2)
    
    if s1==s2 :
        return True
    else:
        list=["'s","s","es","ing","ed","d"]
        for end in list:
            if s2+end == s1:
                return True
        return False

def same(s1,s2):
    "Return True if one of the input strings is the inflection of the other."
    return(is_inflection_of(s1,s2) or is_inflection_of(s2,s1))


def find_match(word,word_list):
    """Given a word, find a string in a list that is "the same" as this word.

    Input:
    - word: a string
    - word_list: a list of stings

    Return value:
    - A string in word_list that is "the same" as word, None otherwise.
    
    The string word is 'the same' as some string x in word_list, if word is the inflection of x,
    ignoring cases and leading or trailing non-alphabetic characters.
    """
    
    if len(word) == 0 or len(word_list) == 0:
        return None
    for w in word_list:
        if same(word, w):
            return w
    return None

def test_strip_non_alpha():
    assert strip_non_alpha("?hello world!") == "hello world"
    assert strip_non_alpha("HELLO ' World3") == "HELLO ' World"
    assert strip_non_alpha("!!@#$%^&*()") == ""
    assert strip_non_alpha("")==""

def test_is_inflection_of():
   assert is_inflection_of("hello", "hello") == True
   assert is_inflection_of("hello", "hell") == False 
   assert is_inflection_of("hellos", "hello") == True
   assert is_inflection_of("helloes", "hello") == True
   assert is_inflection_of("helloing", "hello") == True
   assert is_inflection_of("helloed", "hello") == True
   assert is_inflection_of("hellod", "hello") == True
   assert is_inflection_of("hello's", "hello") == True
   assert is_inflection_of("","") == True

def test_same():
    assert same("hello", "hello") == True
    assert same("hellod", "hello") == True
    assert same("hello", "helloing") == True
    assert same("", "") == True
    assert same("", "a") == False

def test_find_match():
    assert find_match("hello", ["world", "hello", "HELLO"]) == "hello"
    assert find_match("Hello", ["world", "hello", "HELLO"]) == "hello"
    assert find_match("hello", []) == None
    assert find_match("", []) == None



if __name__=="__main__":
    
    # Test strip_non_alpha
    test_strip_non_alpha()
    print("All cases of test_strip_non_alpha passed!")
    # Test is_inflection_of and same
    test_is_inflection_of()
    print("All cases of test_is_inflection_of is  passed!")
    test_same()
    print("All cases of test_same is passed!")
    # Test find_match 
    test_find_match()
    print("All cases of test_find_match passed!")
